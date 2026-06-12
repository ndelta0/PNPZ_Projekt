<script lang="ts">
	import { onDestroy, onMount } from 'svelte';
	import {
		getStatus,
		reconcile,
		scaleBackends,
		configureTraffic as configureServerTraffic,
		startTraffic as startServerTraffic,
		startStack,
		stopTraffic as stopServerTraffic,
		stopStack,
		updateConfig,
		type ContainerStatus,
		type OrchestratorEvent,
		type RuntimeConfig,
		type StatusSnapshot,
		type TrafficConfig,
		type TrafficStats
	} from '$lib/orchestrator';

	const defaultOrchestratorUrl = 'http://localhost:8080';
	const defaultTraffic: TrafficConfig = {
		requests_per_second: 200,
		concurrency: 200,
		random_delay: true,
		random_delay_chance: 20,
		max_random_delay_ms: 750,
		slow_loris: false,
		slow_loris_chance: 5,
		burst_dos: false,
		burst_chance: 5,
		burst_size: 25,
		timeout_ms: 8000,
		key_prefix: 'orch-load'
	};

	let orchestratorUrl = $state(defaultOrchestratorUrl);
	let status = $state<StatusSnapshot | null>(null);
	let configForm = $state<RuntimeConfig | null>(null);
	let configDirty = $state(false);
	let scaleTarget = $state(1);
	let loading = $state(false);
	let connected = $state(false);
	let statusMessage = $state('');
	let errorMessage = $state('');
	let lastUpdated = $state<Date | null>(null);
	let liveEvents = $state<OrchestratorEvent[]>([]);
	let eventSource: EventSource | null = null;
	let pollHandle: ReturnType<typeof setInterval> | undefined;

	let traffic = $state<TrafficConfig>({ ...defaultTraffic });
	let trafficDirty = $state(false);
	let trafficStats = $state<TrafficStats>({
		running: false,
		config: { ...defaultTraffic },
		sent: 0,
		ok: 0,
		failed: 0,
		in_flight: 0,
		slow: 0,
		bursts: 0,
		average_latency_ms: 0,
		actual_rps: 0,
		last_error: ''
	});

	const containers = $derived(status?.containers ?? []);
	const backends = $derived(status?.backends ?? []);
	const recentEvents = $derived(liveEvents.slice(0, 80));
	const dbContainers = $derived(containers.filter((container) => container.service === 'db'));
	const backendContainers = $derived(
		containers.filter((container) => container.service === 'backend')
	);
	const frontendContainers = $derived(
		containers.filter((container) => container.service === 'frontend')
	);

	function setStatus(message: string) {
		statusMessage = message;
		errorMessage = '';
	}

	function setError(error: unknown) {
		errorMessage = error instanceof Error ? error.message : String(error);
		statusMessage = '';
	}

	function applySnapshot(next: StatusSnapshot, replaceForm = false) {
		status = next;
		liveEvents = [...(next.events ?? [])].reverse();
		scaleTarget = next.desired_backend_replicas;
		trafficStats = next.traffic;
		if (!trafficStats.running && !trafficDirty) {
			traffic = { ...traffic, ...next.traffic.config };
		}
		lastUpdated = new Date();
		connected = true;

		if (!configForm || replaceForm || !configDirty) {
			configForm = { ...next.config };
			configDirty = false;
		}
	}

	async function refresh(showMessage = false) {
		try {
			const next = await getStatus(orchestratorUrl);
			applySnapshot(next);
			if (showMessage) {
				setStatus('Status refreshed.');
			}
		} catch (error) {
			connected = false;
			setError(error);
		}
	}

	async function runAction(
		label: string,
		action: () => Promise<StatusSnapshot>,
		replaceForm = false
	) {
		loading = true;
		try {
			const next = await action();
			applySnapshot(next, replaceForm);
			setStatus(label);
		} catch (error) {
			setError(error);
		} finally {
			loading = false;
		}
	}

	function connectEvents() {
		eventSource?.close();
		eventSource = new EventSource(`${orchestratorUrl.replace(/\/$/, '')}/api/events`);

		eventSource.onopen = () => {
			connected = true;
		};
		eventSource.onerror = () => {
			connected = false;
		};

		for (const type of ['info', 'action', 'warning', 'error', 'docker']) {
			eventSource.addEventListener(type, (event) => {
				const data = (event as MessageEvent).data;
				if (typeof data !== 'string') {
					return;
				}
				const parsed = JSON.parse(data) as OrchestratorEvent;
				liveEvents = [parsed, ...liveEvents].slice(0, 120);
			});
		}
	}

	function reconnect() {
		localStorage.setItem('dashboard.orchestratorUrl', orchestratorUrl);
		connectEvents();
		void refresh(true);
	}

	function buildConfigPatch(): Partial<RuntimeConfig> {
		if (!configForm) {
			return {};
		}
		return {
			db_image: configForm.db_image.trim(),
			backend_image: configForm.backend_image.trim(),
			frontend_image: configForm.frontend_image.trim(),
			db_data_path: configForm.db_data_path.trim(),
			frontend_host_port: String(configForm.frontend_host_port).trim(),
			backend_cpus: Number(configForm.backend_cpus),
			backend_min_replicas: Number(configForm.backend_min_replicas),
			backend_max_replicas: Number(configForm.backend_max_replicas),
			scale_up_cpu: Number(configForm.scale_up_cpu),
			scale_down_cpu: Number(configForm.scale_down_cpu),
			scale_up_latency_ms: Number(configForm.scale_up_latency_ms),
			scale_down_latency_ms: Number(configForm.scale_down_latency_ms),
			scale_cooldown_ms: Number(configForm.scale_cooldown_ms),
			reconcile_interval_ms: Number(configForm.reconcile_interval_ms),
			health_interval_ms: Number(configForm.health_interval_ms)
		};
	}

	async function saveConfig() {
		await runAction(
			'Runtime configuration saved.',
			() => updateConfig(orchestratorUrl, buildConfigPatch()),
			true
		);
	}

	async function submitScale() {
		await runAction(
			`Backend target set to ${scaleTarget}.`,
			() => scaleBackends(orchestratorUrl, Number(scaleTarget))
		);
	}

	function plainTrafficConfig(): TrafficConfig {
		return {
			requests_per_second: Number(traffic.requests_per_second),
			concurrency: Number(traffic.concurrency),
			random_delay: Boolean(traffic.random_delay),
			random_delay_chance: Number(traffic.random_delay_chance),
			max_random_delay_ms: Number(traffic.max_random_delay_ms),
			slow_loris: Boolean(traffic.slow_loris),
			slow_loris_chance: Number(traffic.slow_loris_chance),
			burst_dos: Boolean(traffic.burst_dos),
			burst_chance: Number(traffic.burst_chance),
			burst_size: Number(traffic.burst_size),
			timeout_ms: Number(traffic.timeout_ms),
			key_prefix: String(traffic.key_prefix)
		};
	}

	async function configureTraffic() {
		trafficDirty = true;
		const config = plainTrafficConfig();
		localStorage.setItem('dashboard.traffic', JSON.stringify(config));
		if (trafficStats.running) {
			try {
				trafficStats = await configureServerTraffic(orchestratorUrl, config);
			} catch (error) {
				setError(error);
			}
		}
	}

	async function startTraffic() {
		const config = plainTrafficConfig();
		localStorage.setItem('dashboard.traffic', JSON.stringify(config));
		try {
			trafficStats = await startServerTraffic(orchestratorUrl, config);
			trafficDirty = false;
			setStatus('Orchestrator traffic generator started.');
		} catch (error) {
			setError(error);
		}
	}

	async function stopTraffic() {
		try {
			trafficStats = await stopServerTraffic(orchestratorUrl);
			setStatus('Orchestrator traffic generator stopped.');
		} catch (error) {
			setError(error);
		}
	}

	function resetTrafficStats() {
		void stopTraffic();
		trafficStats = {
			running: false,
			config: { ...plainTrafficConfig() },
			sent: 0,
			ok: 0,
			failed: 0,
			in_flight: 0,
			slow: 0,
			bursts: 0,
			average_latency_ms: 0,
			actual_rps: 0,
			last_error: ''
		};
	}

	function markConfigDirty() {
		configDirty = true;
	}

	function formatBytes(value: number) {
		if (!value) {
			return '0 B';
		}
		const units = ['B', 'KB', 'MB', 'GB'];
		let current = value;
		let unit = 0;
		while (current >= 1024 && unit < units.length - 1) {
			current /= 1024;
			unit += 1;
		}
		return `${current.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
	}

	function formatTime(value: string | Date | null) {
		if (!value) {
			return '-';
		}
		return new Date(value).toLocaleTimeString();
	}

	function containerTone(running: boolean, oomKilled: boolean) {
		if (oomKilled) {
			return 'border-red-300 bg-red-50 text-red-900';
		}
		if (running) {
			return 'border-emerald-300 bg-emerald-50 text-emerald-900';
		}
		return 'border-zinc-300 bg-zinc-50 text-zinc-700';
	}

	onMount(() => {
		orchestratorUrl = localStorage.getItem('dashboard.orchestratorUrl') || defaultOrchestratorUrl;
		const storedTraffic = localStorage.getItem('dashboard.traffic');
		if (storedTraffic) {
			traffic = { ...defaultTraffic, ...JSON.parse(storedTraffic) };
		}
		connectEvents();
		void refresh();
		pollHandle = setInterval(() => void refresh(), 5000);
	});

	onDestroy(() => {
		eventSource?.close();
		if (pollHandle) {
			clearInterval(pollHandle);
		}
	});
</script>

<svelte:head>
	<title>Orchestrator Dashboard</title>
	<meta name="description" content="Dashboard for the Docker Engine API orchestrator." />
</svelte:head>

<main class="min-h-screen bg-zinc-100 text-zinc-950">
	<div class="mx-auto flex w-full max-w-7xl flex-col gap-5 px-4 py-5 sm:px-6 lg:px-8">
		<header class="border-b border-zinc-300 pb-5">
			<div class="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
				<div>
					<p class="text-sm font-semibold uppercase tracking-wide text-teal-700">Docker orchestrator</p>
					<h1 class="mt-1 text-3xl font-semibold">Runtime control dashboard</h1>
				</div>

				<div class="grid gap-2 sm:grid-cols-[minmax(260px,420px)_auto]">
					<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
						Orchestrator API
						<input
							class="rounded-lg border-zinc-300 bg-white text-zinc-950 shadow-sm focus:border-teal-600 focus:ring-teal-600"
							bind:value={orchestratorUrl}
						/>
					</label>
					<button
						type="button"
						class="self-end rounded-lg bg-zinc-950 px-4 py-2 text-sm font-semibold text-white hover:bg-zinc-800 disabled:opacity-50"
						disabled={loading}
						onclick={reconnect}
					>
						Connect
					</button>
				</div>
			</div>
		</header>

		{#if statusMessage || errorMessage}
			<section
				class={errorMessage
					? 'rounded-lg border border-red-300 bg-red-50 px-4 py-3 text-sm text-red-900'
					: 'rounded-lg border border-emerald-300 bg-emerald-50 px-4 py-3 text-sm text-emerald-900'}
			>
				{errorMessage || statusMessage}
			</section>
		{/if}

		<section class="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
			<div class="rounded-lg border border-zinc-300 bg-white p-4">
				<p class="text-sm text-zinc-500">Connection</p>
				<p class="mt-2 text-2xl font-semibold">{connected ? 'Online' : 'Offline'}</p>
				<p class="mt-1 text-sm text-zinc-500">Updated {formatTime(lastUpdated)}</p>
			</div>
			<div class="rounded-lg border border-zinc-300 bg-white p-4">
				<p class="text-sm text-zinc-500">Stack</p>
				<p class="mt-2 text-2xl font-semibold">{status?.enabled ? 'Running' : 'Stopped'}</p>
				<p class="mt-1 text-sm text-zinc-500">{status?.project ?? '-'} on {status?.network ?? '-'}</p>
			</div>
			<div class="rounded-lg border border-zinc-300 bg-white p-4">
				<p class="text-sm text-zinc-500">Backend replicas</p>
				<p class="mt-2 text-2xl font-semibold">
					{backendContainers.filter((container) => container.running).length} / {status?.desired_backend_replicas ?? 0}
				</p>
				<p class="mt-1 text-sm text-zinc-500">
					min {status?.backend_min_replicas ?? '-'} max {status?.backend_max_replicas ?? '-'}
				</p>
			</div>
			<div class="rounded-lg border border-zinc-300 bg-white p-4">
				<p class="text-sm text-zinc-500">Backend load</p>
				<p class="mt-2 text-2xl font-semibold">{status?.metrics.average_cpu ?? 0}% CPU</p>
				<p class="mt-1 text-sm text-zinc-500">
					{status?.metrics.average_latency_ms ?? 0} ms avg, {formatBytes(status?.metrics.memory_bytes ?? 0)}
				</p>
			</div>
		</section>

		<section class="grid gap-5 xl:grid-cols-[1.1fr_0.9fr]">
			<div class="rounded-lg border border-zinc-300 bg-white p-4">
				<div class="flex flex-col gap-3 border-b border-zinc-200 pb-4 sm:flex-row sm:items-center sm:justify-between">
					<div>
						<h2 class="text-lg font-semibold">Stack Management</h2>
						<p class="text-sm text-zinc-500">Start, stop, reconcile, and set backend capacity.</p>
					</div>
					<div class="flex flex-wrap gap-2">
						<button
							type="button"
							class="rounded-lg bg-emerald-700 px-3 py-2 text-sm font-semibold text-white hover:bg-emerald-800 disabled:opacity-50"
							disabled={loading}
							onclick={() => runAction('Stack start requested.', () => startStack(orchestratorUrl))}
						>
							Start
						</button>
						<button
							type="button"
							class="rounded-lg bg-red-700 px-3 py-2 text-sm font-semibold text-white hover:bg-red-800 disabled:opacity-50"
							disabled={loading}
							onclick={() => runAction('Stack stop requested.', () => stopStack(orchestratorUrl), true)}
						>
							Stop
						</button>
						<button
							type="button"
							class="rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm font-semibold hover:bg-zinc-100 disabled:opacity-50"
							disabled={loading}
							onclick={() => runAction('Reconcile requested.', () => reconcile(orchestratorUrl))}
						>
							Reconcile
						</button>
					</div>
				</div>

				<form
					class="mt-4 grid gap-3 sm:grid-cols-[1fr_auto]"
					onsubmit={(event) => {
						event.preventDefault();
						void submitScale();
					}}
				>
					<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
						Backend replica target
						<input
							type="number"
							min={status?.backend_min_replicas ?? 0}
							max={status?.backend_max_replicas ?? 1}
							class="rounded-lg border-zinc-300 bg-white text-zinc-950 shadow-sm focus:border-teal-600 focus:ring-teal-600"
							bind:value={scaleTarget}
						/>
					</label>
					<button
						type="submit"
						class="self-end rounded-lg bg-teal-700 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-800 disabled:opacity-50"
						disabled={loading || !status}
					>
						Scale
					</button>
				</form>

				<div class="mt-5 grid gap-3 lg:grid-cols-3">
					{@render ServiceColumn('DB', dbContainers)}
					{@render ServiceColumn('Backend', backendContainers)}
					{@render ServiceColumn('Frontend', frontendContainers)}
				</div>
			</div>

			<div class="rounded-lg border border-zinc-300 bg-white p-4">
				<div class="flex items-center justify-between gap-3 border-b border-zinc-200 pb-4">
					<div>
						<h2 class="text-lg font-semibold">Traffic Generator</h2>
						<p class="text-sm text-zinc-500">Runs inside the orchestrator on the Docker network.</p>
					</div>
					<div
						class={trafficStats.running
							? 'rounded-full bg-emerald-100 px-3 py-1 text-sm font-semibold text-emerald-800'
							: 'rounded-full bg-zinc-100 px-3 py-1 text-sm font-semibold text-zinc-700'}
					>
						{trafficStats.running ? 'Running' : 'Stopped'}
					</div>
				</div>

				<div class="mt-4 grid gap-3 sm:grid-cols-2">
					<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
						Requests / second
						<input
							type="number"
							min="1"
							max="10000"
							class="rounded-lg border-zinc-300 bg-white text-zinc-950 shadow-sm focus:border-teal-600 focus:ring-teal-600"
							bind:value={traffic.requests_per_second}
							oninput={configureTraffic}
						/>
					</label>
					<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
						Concurrency
						<input
							type="number"
							min="1"
							max="5000"
							class="rounded-lg border-zinc-300 bg-white text-zinc-950 shadow-sm focus:border-teal-600 focus:ring-teal-600"
							bind:value={traffic.concurrency}
							oninput={configureTraffic}
						/>
					</label>
					<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
						Timeout ms
						<input
							type="number"
							min="1000"
							class="rounded-lg border-zinc-300 bg-white text-zinc-950 shadow-sm focus:border-teal-600 focus:ring-teal-600"
							bind:value={traffic.timeout_ms}
							oninput={configureTraffic}
						/>
					</label>
					<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
						Random delay chance %
						<input
							type="number"
							min="0"
							max="100"
							class="rounded-lg border-zinc-300 bg-white text-zinc-950 shadow-sm focus:border-teal-600 focus:ring-teal-600"
							bind:value={traffic.random_delay_chance}
							oninput={configureTraffic}
						/>
					</label>
					<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
						Max delay ms
						<input
							type="number"
							min="0"
							class="rounded-lg border-zinc-300 bg-white text-zinc-950 shadow-sm focus:border-teal-600 focus:ring-teal-600"
							bind:value={traffic.max_random_delay_ms}
							oninput={configureTraffic}
						/>
					</label>
					<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
						Slow-loris chance %
						<input
							type="number"
							min="0"
							max="100"
							class="rounded-lg border-zinc-300 bg-white text-zinc-950 shadow-sm focus:border-teal-600 focus:ring-teal-600"
							bind:value={traffic.slow_loris_chance}
							oninput={configureTraffic}
						/>
					</label>
					<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
						Burst chance %
						<input
							type="number"
							min="0"
							max="100"
							class="rounded-lg border-zinc-300 bg-white text-zinc-950 shadow-sm focus:border-teal-600 focus:ring-teal-600"
							bind:value={traffic.burst_chance}
							oninput={configureTraffic}
						/>
					</label>
					<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
						Burst size
						<input
							type="number"
							min="1"
							max="500"
							class="rounded-lg border-zinc-300 bg-white text-zinc-950 shadow-sm focus:border-teal-600 focus:ring-teal-600"
							bind:value={traffic.burst_size}
							oninput={configureTraffic}
						/>
					</label>
				</div>

				<div class="mt-4 grid gap-2 sm:grid-cols-3">
					<label class="flex items-center gap-2 rounded-lg border border-zinc-300 p-2 text-sm">
						<input type="checkbox" bind:checked={traffic.random_delay} onchange={configureTraffic} />
						Random delay
					</label>
					<label class="flex items-center gap-2 rounded-lg border border-zinc-300 p-2 text-sm">
						<input type="checkbox" bind:checked={traffic.slow_loris} onchange={configureTraffic} />
						Slow-loris
					</label>
					<label class="flex items-center gap-2 rounded-lg border border-zinc-300 p-2 text-sm">
						<input type="checkbox" bind:checked={traffic.burst_dos} onchange={configureTraffic} />
						Bursts
					</label>
				</div>

				<div class="mt-4 flex flex-wrap gap-2">
					<button
						type="button"
						class="rounded-lg bg-teal-700 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-800"
						onclick={startTraffic}
					>
						Start Traffic
					</button>
					<button
						type="button"
						class="rounded-lg border border-zinc-300 px-4 py-2 text-sm font-semibold hover:bg-zinc-100"
						onclick={stopTraffic}
					>
						Stop
					</button>
					<button
						type="button"
						class="rounded-lg border border-zinc-300 px-4 py-2 text-sm font-semibold hover:bg-zinc-100"
						onclick={resetTrafficStats}
					>
						Reset Stats
					</button>
				</div>

				<div class="mt-4 grid grid-cols-2 gap-3 text-sm lg:grid-cols-4">
					{@render Stat('Sent', String(trafficStats.sent))}
					{@render Stat('OK', String(trafficStats.ok))}
					{@render Stat('Failed', String(trafficStats.failed))}
					{@render Stat('In flight', String(trafficStats.in_flight))}
					{@render Stat('Actual RPS', String(trafficStats.actual_rps))}
					{@render Stat('Avg latency', `${trafficStats.average_latency_ms} ms`)}
					{@render Stat('Slow', String(trafficStats.slow))}
					{@render Stat('Bursts', String(trafficStats.bursts))}
				</div>
				{#if trafficStats.last_error}
					<p class="mt-3 break-all rounded-lg bg-red-50 p-2 text-sm text-red-900">
						{trafficStats.last_error}
					</p>
				{/if}
			</div>
		</section>

		<section class="grid gap-5 xl:grid-cols-[0.95fr_1.05fr]">
			<div class="rounded-lg border border-zinc-300 bg-white p-4">
				<div class="flex items-center justify-between gap-3 border-b border-zinc-200 pb-4">
					<div>
						<h2 class="text-lg font-semibold">Runtime Configuration</h2>
						<p class="text-sm text-zinc-500">Mutable orchestrator settings. Some changes recreate containers.</p>
					</div>
					<button
						type="button"
						class="rounded-lg bg-zinc-950 px-4 py-2 text-sm font-semibold text-white hover:bg-zinc-800 disabled:opacity-50"
						disabled={!configForm || loading}
						onclick={saveConfig}
					>
						Save
					</button>
				</div>

				{#if configForm}
					<div class="mt-4 grid gap-3 sm:grid-cols-2">
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Project
							<input class="rounded-lg border-zinc-200 bg-zinc-100 text-zinc-500" value={configForm.project_name} readonly />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Network
							<input class="rounded-lg border-zinc-200 bg-zinc-100 text-zinc-500" value={configForm.network_name} readonly />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							DB image
							<input class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.db_image} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Backend image
							<input class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.backend_image} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Frontend image
							<input class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.frontend_image} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Frontend host port
							<input class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.frontend_host_port} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700 sm:col-span-2">
							DB JSON path
							<input class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.db_data_path} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Backend min
							<input type="number" min="0" class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.backend_min_replicas} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Backend max
							<input type="number" min="1" class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.backend_max_replicas} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Backend CPU limit
							<input type="number" min="0" step="0.05" class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.backend_cpus} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Scale up CPU %
							<input type="number" min="0" class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.scale_up_cpu} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Scale down CPU %
							<input type="number" min="0" class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.scale_down_cpu} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Scale up latency ms
							<input type="number" min="1" class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.scale_up_latency_ms} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Scale down latency ms
							<input type="number" min="1" class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.scale_down_latency_ms} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Cooldown ms
							<input type="number" min="1" class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.scale_cooldown_ms} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Health interval ms
							<input type="number" min="1" class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.health_interval_ms} oninput={markConfigDirty} />
						</label>
						<label class="flex flex-col gap-1 text-sm font-medium text-zinc-700">
							Reconcile interval ms
							<input type="number" min="1" class="rounded-lg border-zinc-300 bg-white text-zinc-950" bind:value={configForm.reconcile_interval_ms} oninput={markConfigDirty} />
						</label>
					</div>
				{/if}
			</div>

			<div class="rounded-lg border border-zinc-300 bg-white p-4">
				<h2 class="border-b border-zinc-200 pb-4 text-lg font-semibold">Backends and Events</h2>
				<div class="mt-4 overflow-x-auto">
					<table class="min-w-full text-left text-sm">
						<thead class="border-b border-zinc-200 text-zinc-500">
							<tr>
								<th class="py-2 pr-3">Replica</th>
								<th class="py-2 pr-3">IP</th>
								<th class="py-2 pr-3">Health</th>
								<th class="py-2 pr-3">Latency</th>
							</tr>
						</thead>
						<tbody>
							{#each backends as backend (backend.id)}
								<tr class="border-b border-zinc-100">
									<td class="py-2 pr-3 font-medium">{backend.name}</td>
									<td class="py-2 pr-3">{backend.ip_address}</td>
									<td class="py-2 pr-3">{backend.healthy ? 'healthy' : 'unhealthy'}</td>
									<td class="py-2 pr-3">{backend.latency_ms} ms</td>
								</tr>
							{/each}
							{#if backends.length === 0}
								<tr><td class="py-4 text-zinc-500" colspan="4">No backend replicas reported.</td></tr>
							{/if}
						</tbody>
					</table>
				</div>

				<div class="mt-5 max-h-96 overflow-auto rounded-lg border border-zinc-200">
					{#each recentEvents as event, index (`${event.time}-${event.message}-${index}`)}
						<div class="border-b border-zinc-100 px-3 py-2 text-sm last:border-b-0">
							<div class="flex flex-wrap items-center gap-2">
								<span class="font-mono text-xs text-zinc-500">{formatTime(event.time)}</span>
								<span class="rounded bg-zinc-100 px-2 py-0.5 text-xs font-semibold uppercase text-zinc-700">{event.type}</span>
								{#if event.service}
									<span class="rounded bg-teal-50 px-2 py-0.5 text-xs font-semibold text-teal-800">{event.service}</span>
								{/if}
							</div>
							<p class="mt-1 text-zinc-900">{event.message}</p>
						</div>
					{/each}
					{#if recentEvents.length === 0}
						<p class="p-4 text-sm text-zinc-500">No events yet.</p>
					{/if}
				</div>
			</div>
		</section>
	</div>
</main>

{#snippet ServiceColumn(title: string, containers: ContainerStatus[])}
	<div class="rounded-lg border border-zinc-200 p-3">
		<h3 class="text-sm font-semibold uppercase tracking-wide text-zinc-500">{title}</h3>
		<div class="mt-3 flex flex-col gap-2">
			{#each containers as container (container.id)}
				<div class={`rounded-lg border p-3 text-sm ${containerTone(container.running, container.oom_killed)}`}>
					<p class="font-semibold">{container.name}</p>
					<p class="mt-1 break-all text-xs">{container.status}</p>
					<p class="mt-1 text-xs">exit {container.exit_code} {container.ip_address ? `- ${container.ip_address}` : ''}</p>
				</div>
			{/each}
			{#if containers.length === 0}
				<p class="rounded-lg border border-dashed border-zinc-300 p-3 text-sm text-zinc-500">No containers.</p>
			{/if}
		</div>
	</div>
{/snippet}

{#snippet Stat(label: string, value: string)}
	<div class="rounded-lg border border-zinc-200 p-3">
		<p class="text-xs font-semibold uppercase tracking-wide text-zinc-500">{label}</p>
		<p class="mt-1 text-lg font-semibold">{value}</p>
	</div>
{/snippet}

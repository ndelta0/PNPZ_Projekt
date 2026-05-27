<script lang="ts">
	import { onDestroy, onMount } from 'svelte';
	import {
		backendUrl,
		createItem,
		deleteItem,
		getHealth,
		getItem,
		listItems,
		updateItem,
		type KeyValueItem
	} from '$lib/api';

	let items = $state<KeyValueItem[]>([]);
	let health = $state<'unknown' | 'ok' | 'down'>('unknown');
	let loading = $state(false);
	let autoRefresh = $state(true);
	let statusMessage = $state('');
	let errorMessage = $state('');
	let lastUpdated = $state<Date | null>(null);

	let formKey = $state('');
	let formValue = $state('');
	let lookupKey = $state('');
	let lookupResult = $state<KeyValueItem | null>(null);

	let pollHandle: ReturnType<typeof setInterval> | undefined;

	const sortedItems = $derived([...items].sort((a, b) => a.key.localeCompare(b.key)));

	function setStatus(message: string) {
		statusMessage = message;
		errorMessage = '';
	}

	function setError(error: unknown) {
		errorMessage = error instanceof Error ? error.message : String(error);
		statusMessage = '';
	}

	async function refreshHealth() {
		try {
			const response = await getHealth();
			health = response.status === 'ok' ? 'ok' : 'down';
		} catch (error) {
			health = 'down';
			console.error('Health check failed:', error);
		}
	}

	async function refreshItems(showStatus = false) {
		loading = true;
		try {
			items = await listItems();
			lastUpdated = new Date();
			if (showStatus) {
				setStatus(`Loaded ${items.length} item${items.length === 1 ? '' : 's'}.`);
			}
		} catch (error) {
			setError(error);
		} finally {
			loading = false;
		}
	}

	async function refreshAll(showStatus = false) {
		await Promise.all([refreshHealth(), refreshItems(showStatus)]);
	}

	async function saveItem() {
		const key = formKey.trim();

		if (!key) {
			setError('Key is required.');
			return;
		}

		loading = true;
		try {
			const exists = items.some((item) => item.key === key);

			if (exists) {
				await updateItem(key, formValue);
				setStatus(`Updated key "${key}".`);
			} else {
				await createItem({ key, value: formValue });
				setStatus(`Created key "${key}".`);
			}

			formKey = '';
			formValue = '';
			await refreshItems();
		} catch (error) {
			setError(error);
		} finally {
			loading = false;
		}
	}

	async function readItem() {
		const key = lookupKey.trim();

		if (!key) {
			setError('Lookup key is required.');
			return;
		}

		loading = true;
		try {
			lookupResult = await getItem(key);
			setStatus(`Read key "${key}".`);
		} catch (error) {
			lookupResult = null;
			setError(error);
		} finally {
			loading = false;
		}
	}

	async function removeItem(key: string) {
		if (!key) {
			return;
		}

		loading = true;
		try {
			await deleteItem(key);
			if (lookupResult?.key === key) {
				lookupResult = null;
			}
			setStatus(`Deleted key "${key}".`);
			await refreshItems();
		} catch (error) {
			setError(error);
		} finally {
			loading = false;
		}
	}

	function editItem(item: KeyValueItem) {
		formKey = item.key;
		formValue = item.value;
		setStatus(`Editing key "${item.key}".`);
	}

	function clearMessages() {
		statusMessage = '';
		errorMessage = '';
	}

	function startPolling() {
		stopPolling();
		pollHandle = setInterval(() => {
			if (autoRefresh) {
				void refreshAll();
			}
		}, 3000);
	}

	function stopPolling() {
		if (pollHandle) {
			clearInterval(pollHandle);
			pollHandle = undefined;
		}
	}

	onMount(() => {
		void refreshAll(true);
		startPolling();
	});

	onDestroy(() => {
		stopPolling();
	});
</script>

<svelte:head>
	<title>Dummy Key-Value Monitor</title>
	<meta
			name="description"
			content="Simple Svelte frontend for monitoring and managing dummy backend key-value pairs."
	/>
</svelte:head>

<main class="min-h-screen bg-slate-950 text-slate-100">
	<section class="mx-auto flex w-full max-w-6xl flex-col gap-6 px-4 py-8 sm:px-6 lg:px-8">
		<header class="flex flex-col gap-4 rounded-3xl border border-slate-800 bg-slate-900/80 p-6 shadow-2xl shadow-black/30 md:flex-row md:items-center md:justify-between">
			<div>
				<p class="text-sm font-semibold uppercase tracking-[0.25em] text-cyan-300">Dummy Frontend</p>
				<h1 class="mt-2 text-3xl font-bold tracking-tight sm:text-4xl">Key-Value Store Monitor</h1>
				<p class="mt-3 max-w-2xl text-sm text-slate-300">
					Create, update, read, delete, and monitor key-value pairs stored through the backend service.
				</p>
			</div>

			<div class="rounded-2xl border border-slate-700 bg-slate-950/70 p-4 text-sm">
				<div class="flex items-center gap-2">
					<span
							class={[
							'h-3 w-3 rounded-full',
							health === 'ok' ? 'bg-emerald-400' : health === 'down' ? 'bg-red-400' : 'bg-amber-400'
						]}
					></span>
					<span class="font-semibold">
						Backend:
						{health === 'ok' ? 'healthy' : health === 'down' ? 'unreachable' : 'unknown'}
					</span>
				</div>
				<p class="mt-2 max-w-xs break-all text-slate-400">{backendUrl}</p>
			</div>
		</header>

		{#if statusMessage || errorMessage}
			<div
					class={[
					'flex items-start justify-between gap-4 rounded-2xl border px-4 py-3 text-sm',
					errorMessage
						? 'border-red-700 bg-red-950/60 text-red-100'
						: 'border-emerald-700 bg-emerald-950/60 text-emerald-100'
				]}
			>
				<p>{errorMessage || statusMessage}</p>
				<button
						type="button"
						class="rounded-lg px-2 py-1 text-xs font-semibold hover:bg-white/10"
						onclick={clearMessages}
				>
					Dismiss
				</button>
			</div>
		{/if}

		<div class="grid gap-6 lg:grid-cols-[1fr_1fr]">
			<section class="rounded-3xl border border-slate-800 bg-slate-900 p-6">
				<h2 class="text-xl font-bold">Set key-value pair</h2>
				<p class="mt-1 text-sm text-slate-400">
					Uses create when the key does not exist, otherwise update.
				</p>

				<form class="mt-5 flex flex-col gap-4" onsubmit={(event) => { event.preventDefault(); void saveItem(); }}>
					<label class="flex flex-col gap-2">
						<span class="text-sm font-medium text-slate-300">Key</span>
						<input
								class="rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100 outline-none ring-cyan-400/30 transition focus:border-cyan-400 focus:ring-4"
								bind:value={formKey}
								maxlength="256"
								placeholder="example-key"
						/>
					</label>

					<label class="flex flex-col gap-2">
						<span class="text-sm font-medium text-slate-300">Value</span>
						<textarea
								class="min-h-28 rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100 outline-none ring-cyan-400/30 transition focus:border-cyan-400 focus:ring-4"
								bind:value={formValue}
								placeholder="example-value"
						></textarea>
					</label>

					<div class="flex flex-wrap gap-3">
						<button
								type="submit"
								disabled={loading}
								class="rounded-xl bg-cyan-400 px-5 py-3 font-bold text-slate-950 transition hover:bg-cyan-300 disabled:cursor-not-allowed disabled:opacity-50"
						>
							Save
						</button>
						<button
								type="button"
								class="rounded-xl border border-slate-700 px-5 py-3 font-bold text-slate-200 transition hover:bg-slate-800"
								onclick={() => {
								formKey = '';
								formValue = '';
							}}
						>
							Clear
						</button>
					</div>
				</form>
			</section>

			<section class="rounded-3xl border border-slate-800 bg-slate-900 p-6">
				<h2 class="text-xl font-bold">Read one key</h2>
				<p class="mt-1 text-sm text-slate-400">Fetch a single value directly from the backend.</p>

				<form class="mt-5 flex flex-col gap-4" onsubmit={(event) => { event.preventDefault(); void readItem(); }}>
					<label class="flex flex-col gap-2">
						<span class="text-sm font-medium text-slate-300">Lookup key</span>
						<input
								class="rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100 outline-none ring-cyan-400/30 transition focus:border-cyan-400 focus:ring-4"
								bind:value={lookupKey}
								placeholder="example-key"
						/>
					</label>

					<button
							type="submit"
							disabled={loading}
							class="w-fit rounded-xl bg-violet-400 px-5 py-3 font-bold text-slate-950 transition hover:bg-violet-300 disabled:cursor-not-allowed disabled:opacity-50"
					>
						Read
					</button>
				</form>

				{#if lookupResult}
					<div class="mt-5 rounded-2xl border border-slate-700 bg-slate-950 p-4">
						<p class="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Result</p>
						<dl class="mt-3 grid gap-3">
							<div>
								<dt class="text-sm text-slate-400">Key</dt>
								<dd class="break-all font-mono text-cyan-200">{lookupResult.key}</dd>
							</div>
							<div>
								<dt class="text-sm text-slate-400">Value</dt>
								<dd class="whitespace-pre-wrap break-words font-mono text-slate-100">{lookupResult.value}</dd>
							</div>
						</dl>
					</div>
				{/if}
			</section>
		</div>

		<section class="rounded-3xl border border-slate-800 bg-slate-900 p-6">
			<div class="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
				<div>
					<h2 class="text-xl font-bold">Live key-value pairs</h2>
					<p class="mt-1 text-sm text-slate-400">
						{items.length} item{items.length === 1 ? '' : 's'}
						{#if lastUpdated}
							<span> · Last updated {lastUpdated.toLocaleTimeString()}</span>
						{/if}
					</p>
				</div>

				<div class="flex flex-wrap items-center gap-3">
					<label class="flex cursor-pointer items-center gap-2 rounded-xl border border-slate-700 px-4 py-3 text-sm">
						<input type="checkbox" bind:checked={autoRefresh} />
						<span>Auto-refresh</span>
					</label>

					<button
							type="button"
							disabled={loading}
							class="rounded-xl border border-slate-700 px-5 py-3 font-bold transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
							onclick={() => void refreshAll(true)}
					>
						{loading ? 'Loading...' : 'Refresh'}
					</button>
				</div>
			</div>

			<div class="mt-6 overflow-hidden rounded-2xl border border-slate-800">
				{#if sortedItems.length === 0}
					<div class="bg-slate-950 p-8 text-center text-slate-400">
						No key-value pairs yet. Create one above.
					</div>
				{:else}
					<div class="overflow-x-auto">
						<table class="w-full min-w-[720px] border-collapse text-left text-sm">
							<thead class="bg-slate-950 text-xs uppercase tracking-[0.2em] text-slate-400">
							<tr>
								<th class="px-4 py-4">Key</th>
								<th class="px-4 py-4">Value</th>
								<th class="px-4 py-4 text-right">Actions</th>
							</tr>
							</thead>
							<tbody class="divide-y divide-slate-800">
							{#each sortedItems as item}
								<tr class="bg-slate-900/70 transition hover:bg-slate-800/70">
									<td class="max-w-xs break-all px-4 py-4 font-mono text-cyan-200">{item.key}</td>
									<td class="max-w-md whitespace-pre-wrap break-words px-4 py-4 font-mono text-slate-200">
										{item.value}
									</td>
									<td class="px-4 py-4">
										<div class="flex justify-end gap-2">
											<button
													type="button"
													class="rounded-lg border border-slate-700 px-3 py-2 font-semibold transition hover:bg-slate-700"
													onclick={() => editItem(item)}
											>
												Edit
											</button>
											<button
													type="button"
													class="rounded-lg border border-red-800 px-3 py-2 font-semibold text-red-200 transition hover:bg-red-950"
													onclick={() => void removeItem(item.key)}
											>
												Delete
											</button>
										</div>
									</td>
								</tr>
							{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</div>
		</section>
	</section>
</main>
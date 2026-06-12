export type RuntimeConfig = {
	http_addr: string;
	backend_proxy_addr: string;
	project_name: string;
	network_name: string;
	db_image: string;
	backend_image: string;
	frontend_image: string;
	db_data_path: string;
	frontend_host_port: string;
	backend_cpus: number;
	backend_min_replicas: number;
	backend_max_replicas: number;
	scale_up_cpu: number;
	scale_down_cpu: number;
	scale_up_latency_ms: number;
	scale_down_latency_ms: number;
	scale_cooldown_ms: number;
	reconcile_interval_ms: number;
	health_interval_ms: number;
};

export type OrchestratorEvent = {
	time: string;
	type: string;
	service?: string;
	message: string;
	details?: Record<string, unknown>;
};

export type BackendRef = {
	id: string;
	name: string;
	ip_address: string;
	healthy: boolean;
	latency_ms: number;
	last_check: string;
};

export type ContainerStatus = {
	id: string;
	name: string;
	service: string;
	image: string;
	state: string;
	status: string;
	running: boolean;
	oom_killed: boolean;
	exit_code: number;
	ip_address?: string;
};

export type Metrics = {
	average_cpu: number;
	average_latency_ms: number;
	memory_bytes: number;
};

export type TrafficConfig = {
	requests_per_second: number;
	concurrency: number;
	random_delay: boolean;
	random_delay_chance: number;
	max_random_delay_ms: number;
	slow_loris: boolean;
	slow_loris_chance: number;
	burst_dos: boolean;
	burst_chance: number;
	burst_size: number;
	timeout_ms: number;
	key_prefix: string;
};

export type TrafficStats = {
	running: boolean;
	config: TrafficConfig;
	started_at?: string;
	sent: number;
	ok: number;
	failed: number;
	in_flight: number;
	slow: number;
	bursts: number;
	average_latency_ms: number;
	actual_rps: number;
	last_error?: string;
};

export type StatusSnapshot = {
	project: string;
	network: string;
	enabled: boolean;
	config: RuntimeConfig;
	traffic: TrafficStats;
	desired_backend_replicas: number;
	backend_min_replicas: number;
	backend_max_replicas: number;
	metrics: Metrics;
	backends: BackendRef[];
	containers: ContainerStatus[];
	events: OrchestratorEvent[];
};

export type RuntimeConfigPatch = Partial<
	Pick<
		RuntimeConfig,
		| 'db_image'
		| 'backend_image'
		| 'frontend_image'
		| 'db_data_path'
		| 'frontend_host_port'
		| 'backend_cpus'
		| 'backend_min_replicas'
		| 'backend_max_replicas'
		| 'scale_up_cpu'
		| 'scale_down_cpu'
		| 'scale_up_latency_ms'
		| 'scale_down_latency_ms'
		| 'scale_cooldown_ms'
		| 'reconcile_interval_ms'
		| 'health_interval_ms'
	>
>;

async function parseResponse<T>(response: Response): Promise<T> {
	const contentType = response.headers.get('content-type') ?? '';
	const body = contentType.includes('application/json') ? await response.json() : await response.text();

	if (!response.ok) {
		const message =
			typeof body === 'object' && body !== null && 'error' in body
				? String(body.error)
				: typeof body === 'string'
					? body
					: `HTTP ${response.status}`;
		throw new Error(message);
	}

	return body as T;
}

function base(url: string): string {
	return url.replace(/\/$/, '');
}

export async function getStatus(orchestratorUrl: string): Promise<StatusSnapshot> {
	const response = await fetch(`${base(orchestratorUrl)}/api/status`);
	return parseResponse<StatusSnapshot>(response);
}

export async function startStack(orchestratorUrl: string): Promise<StatusSnapshot> {
	const response = await fetch(`${base(orchestratorUrl)}/api/start`, { method: 'POST' });
	return parseResponse<StatusSnapshot>(response);
}

export async function stopStack(orchestratorUrl: string): Promise<StatusSnapshot> {
	const response = await fetch(`${base(orchestratorUrl)}/api/stop`, { method: 'POST' });
	return parseResponse<StatusSnapshot>(response);
}

export async function reconcile(orchestratorUrl: string): Promise<StatusSnapshot> {
	const response = await fetch(`${base(orchestratorUrl)}/api/reconcile`, { method: 'POST' });
	return parseResponse<StatusSnapshot>(response);
}

export async function scaleBackends(
	orchestratorUrl: string,
	backendReplicas: number
): Promise<StatusSnapshot> {
	const response = await fetch(`${base(orchestratorUrl)}/api/scale`, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ backend_replicas: backendReplicas })
	});
	return parseResponse<StatusSnapshot>(response);
}

export async function updateConfig(
	orchestratorUrl: string,
	patch: RuntimeConfigPatch
): Promise<StatusSnapshot> {
	const response = await fetch(`${base(orchestratorUrl)}/api/config`, {
		method: 'PATCH',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify(patch)
	});
	return parseResponse<StatusSnapshot>(response);
}

export async function startTraffic(
	orchestratorUrl: string,
	config: TrafficConfig
): Promise<TrafficStats> {
	const response = await fetch(`${base(orchestratorUrl)}/api/traffic`, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ action: 'start', config })
	});
	return parseResponse<TrafficStats>(response);
}

export async function configureTraffic(
	orchestratorUrl: string,
	config: TrafficConfig
): Promise<TrafficStats> {
	const response = await fetch(`${base(orchestratorUrl)}/api/traffic`, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ action: 'configure', config })
	});
	return parseResponse<TrafficStats>(response);
}

export async function stopTraffic(orchestratorUrl: string): Promise<TrafficStats> {
	const response = await fetch(`${base(orchestratorUrl)}/api/traffic`, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ action: 'stop' })
	});
	return parseResponse<TrafficStats>(response);
}

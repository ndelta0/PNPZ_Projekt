import { PUBLIC_BACKEND_URL } from '$env/static/public';

export type KeyValueItem = {
    key: string;
    value: string;
};

export type HealthResponse = {
    status: string;
};

const fallbackBackendUrl = 'http://localhost:8000';

export const backendUrl = (PUBLIC_BACKEND_URL || fallbackBackendUrl).replace(/\/$/, '');

async function parseResponse<T>(response: Response): Promise<T> {
    const contentType = response.headers.get('content-type') ?? '';
    const isJson = contentType.includes('application/json');
    const body = isJson ? await response.json() : await response.text();

    if (!response.ok) {
        const detail =
            typeof body === 'object' && body !== null && 'detail' in body
                ? String(body.detail)
                : typeof body === 'string'
                    ? body
                    : `HTTP ${response.status}`;

        throw new Error(detail || `Request failed with HTTP ${response.status}`);
    }

    return body as T;
}

export async function getHealth(): Promise<HealthResponse> {
    const response = await fetch(`${backendUrl}/health`);
    return parseResponse<HealthResponse>(response);
}

export async function listItems(): Promise<KeyValueItem[]> {
    const response = await fetch(`${backendUrl}/items`);
    return parseResponse<KeyValueItem[]>(response);
}

export async function getItem(key: string): Promise<KeyValueItem> {
    const response = await fetch(`${backendUrl}/items/${encodeURIComponent(key)}`);
    return parseResponse<KeyValueItem>(response);
}

export async function createItem(item: KeyValueItem): Promise<KeyValueItem> {
    const response = await fetch(`${backendUrl}/items`, {
        method: 'POST',
        headers: {
            'content-type': 'application/json'
        },
        body: JSON.stringify(item)
    });

    return parseResponse<KeyValueItem>(response);
}

export async function updateItem(key: string, value: string): Promise<KeyValueItem> {
    const response = await fetch(`${backendUrl}/items/${encodeURIComponent(key)}`, {
        method: 'PUT',
        headers: {
            'content-type': 'application/json'
        },
        body: JSON.stringify({ value })
    });

    return parseResponse<KeyValueItem>(response);
}

export async function deleteItem(key: string): Promise<{ deleted: boolean; key: string }> {
    const response = await fetch(`${backendUrl}/items/${encodeURIComponent(key)}`, {
        method: 'DELETE'
    });

    return parseResponse<{ deleted: boolean; key: string }>(response);
}
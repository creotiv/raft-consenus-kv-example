import http from 'k6/http';
import { check, sleep } from 'k6';

// Simple test configuration for quick testing
export const options = {
    vus: 100,        // 5 virtual users
    duration: '30s', // Run for 1 minute
};

// Base URL for the KV service
var BASE_URL = __ENV.BASE_URL || 'http://localhost:7103';

// Helper function to generate random strings
function generateRandomString(length = 8) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

// Helper function to generate random key-value pairs
function generateRandomKV() {
    const keyLength = Math.floor(Math.random() * 10) + 3; // 3-12 characters
    const valueLength = Math.floor(Math.random() * 20) + 5; // 5-24 characters

    return {
        key: generateRandomString(keyLength),
        value: generateRandomString(valueLength)
    };
}

// Main test function - focuses on SET operations with random keys/values
export default function () {
    const kv = generateRandomKV();

    // Test: Set key-value pair (equivalent to your curl command)
    var setResponse = http.post(`${BASE_URL}/kv/set`, JSON.stringify(kv), {
        headers: {
            'Content-Type': 'application/json',
        },
    });
    if (setResponse.status === 421) {
        // Automatically switch to the leader's port
        const leaderUrl = setResponse.headers['X-URL'] || setResponse.headers['x-url'] || setResponse.headers['X-Url'];
        if (leaderUrl) {
            console.log(`Redirecting to leader: ${leaderUrl}`);
            BASE_URL = leaderUrl;
            setResponse = http.post(`${BASE_URL}/kv/set`, JSON.stringify(kv), {
                headers: {
                    'Content-Type': 'application/json',
                },
            });
        } else {
            console.log('No leader URL in headers, staying with current BASE_URL');
        }
    }

    check(setResponse, {
        'NOT_LEADER': (r) => r.json('detail') === 'NOT_LEADER',
        'COMMIT_TIMEOUT': (r) => r.json('detail') === 'COMMIT_TIMEOUT',
        'SYSTEM_OVERLOADED': (r) => r.json('detail') === 'SYSTEM_OVERLOADED',
        'CORRUPT_LOG': (r) => r.json('detail') === 'CORRUPT_LOG',
        'set status is 200': (r) => r.status === 200,
        'set response has ok field': (r) => r.json('ok') === true,
    });

    // Random sleep between 0.1 and 0.5 seconds between requests
    sleep(Math.random() * 0.2);
}

// Setup function (runs once before the test)
export function setup() {
    console.log(`Starting simple k6 test against KV service at ${BASE_URL}`);
    console.log('Test will run with 5 parallel users performing SET operations');
    console.log('Each user will generate random keys and values for testing');
    console.log('Equivalent to: curl -X POST localhost:7102/kv/set -d \'{"key":"random","value":"random"}\' -H "Content-Type: application/json"');
}

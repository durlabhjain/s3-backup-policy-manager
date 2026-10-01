import { existsSync, readFileSync } from 'node:fs';

const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
export function mergeConfig(base, override) {
    const result = Object.create(null);
    for (const source of [base, override]) {
        for (const [key, value] of Object.entries(source || {})) {
            if (['__proto__', 'constructor', 'prototype'].includes(key)) throw new Error('Unsafe configuration key');
            result[key] = object(value) ? mergeConfig(object(result[key]) ? result[key] : {}, value) : value;
        }
    }
    return result;
}

export function loadApplicationConfig(path = 'app-config.json', explicit = false) {
    if (!existsSync(path)) {
        if (explicit) throw new Error('Application config file does not exist');
        return { logging: { file: { enabled: false } }, telemetry: { enabled: false }, jobDefaults: {} };
    }
    let config; try { config = JSON.parse(readFileSync(path, 'utf8')); } catch { throw new Error('Invalid application config JSON'); }
    return validateApplicationConfig(config);
}

export function validateApplicationConfig(config) {
    if (!object(config)) throw new Error('Application config must be an object');
    if (config.jobDefaults !== undefined && !object(config.jobDefaults)) throw new Error('jobDefaults must be an object');
    // A shared default must never authorize destructive operations for another job.
    if (config.jobDefaults?.dryRun === false || config.jobDefaults?.deleteNonRetained === true)
        throw new Error('Deletion authorization must be configured per job, not in jobDefaults');
    return config;
}

export function applicationDefaults(config) {
    validateApplicationConfig(config);
    return mergeConfig({ logging: { file: { enabled: false, directory: 'logs', retentionDays: 30 } },
        telemetry: { enabled: false }, outputDirectory: 'output', jobDefaults: {} }, config);
}

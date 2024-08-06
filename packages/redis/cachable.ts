import { getRedisCache } from './redis';

export function cacheable<T extends (...args: any) => any>(
  fn: T,
  expireInSec: number
) {
  return async function (
    ...args: Parameters<T>
  ): Promise<Awaited<ReturnType<T>>> {
    // JSON.stringify here is not bullet proof since ordering of object keys matters etc
    const key = `cachable:${fn.name}:${JSON.stringify(args)}`;
    const cached = await getRedisCache().get(key);
    if (cached) {
      try {
        return JSON.parse(cached);
      } catch (e) {
        console.error('Failed to parse cache', e);
      }
    }
    const result = await fn(...(args as any));

    if (result !== undefined || result !== null) {
      getRedisCache().setex(key, expireInSec, JSON.stringify(result));
    }

    return result;
  };
}

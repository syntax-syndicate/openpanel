import { Expression } from './clickhouse-query';

// Helper function to wrap expressions (keep this private)
function wrap(expr: string | Expression): string {
  return expr instanceof Expression ? expr.toString() : expr;
}

export const fn = {
  // Date & Time Functions
  toDate: (expr: string | Expression): Expression => {
    return new Expression(`toDate(${wrap(expr)})`);
  },

  toDateTime: (expr: string | Expression): Expression => {
    return new Expression(`toDateTime(${wrap(expr)})`);
  },

  now: (): Expression => {
    return new Expression('now()');
  },

  today: (): Expression => {
    return new Expression('today()');
  },

  yesterday: (): Expression => {
    return new Expression('yesterday()');
  },

  toStartOfHour: (expr: string | Expression): Expression => {
    return new Expression(`toStartOfHour(${wrap(expr)})`);
  },

  toStartOfDay: (expr: string | Expression): Expression => {
    return new Expression(`toStartOfDay(${wrap(expr)})`);
  },

  toStartOfMonth: (expr: string | Expression): Expression => {
    return new Expression(`toStartOfMonth(${wrap(expr)})`);
  },

  toStartOfYear: (expr: string | Expression): Expression => {
    return new Expression(`toStartOfYear(${wrap(expr)})`);
  },

  dateDiff: (
    unit: 'second' | 'minute' | 'hour' | 'day' | 'week' | 'month' | 'year',
    start: string | Expression,
    end: string | Expression,
  ): Expression => {
    return new Expression(`dateDiff('${unit}', ${wrap(start)}, ${wrap(end)})`);
  },

  // Aggregate Functions
  count: (expr: string | Expression = '*'): Expression => {
    return new Expression(`count(${wrap(expr)})`);
  },

  countDistinct: (expr: string | Expression): Expression => {
    return new Expression(`count(DISTINCT ${wrap(expr)})`);
  },

  min: (expr: string | Expression): Expression => {
    return new Expression(`min(${wrap(expr)})`);
  },

  max: (expr: string | Expression, as?: string): Expression => {
    return new Expression(`max(${wrap(expr)})${as ? ` as ${as}` : ''}`);
  },

  sum: (expr: string | Expression): Expression => {
    return new Expression(`sum(${wrap(expr)})`);
  },

  avg: (expr: string | Expression): Expression => {
    return new Expression(`avg(${wrap(expr)})`);
  },

  distinct: (expr: string | Expression): Expression => {
    return new Expression(`distinct(${wrap(expr)})`);
  },

  // String Functions
  concat: (...args: (string | Expression)[]): Expression => {
    return new Expression(`concat(${args.map((arg) => wrap(arg)).join(', ')})`);
  },

  substring: (
    expr: string | Expression,
    start: number,
    length?: number,
  ): Expression => {
    const params = length
      ? `${wrap(expr)}, ${start}, ${length}`
      : `${wrap(expr)}, ${start}`;
    return new Expression(`substring(${params})`);
  },

  // Array Functions
  arrayJoin: (expr: string | Expression): Expression => {
    return new Expression(`arrayJoin(${wrap(expr)})`);
  },

  arrayLength: (expr: string | Expression): Expression => {
    return new Expression(`length(${wrap(expr)})`);
  },

  // Conditional Functions
  ifThen: (
    condition: string | Expression,
    then: string | Expression,
    else_: string | Expression,
  ): Expression => {
    return new Expression(
      `if(${wrap(condition)}, ${wrap(then)}, ${wrap(else_)})`,
    );
  },
};

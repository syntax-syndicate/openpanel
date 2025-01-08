import type { ClickHouseClient } from '@clickhouse/client';
import { escape } from 'sqlstring';

type SqlValue = string | number | boolean | Date | null;
type SqlParam = SqlValue | SqlValue[];
type Operator =
  | '='
  | '>'
  | '<'
  | '>='
  | '<='
  | '!='
  | 'IN'
  | 'NOT IN'
  | 'LIKE'
  | 'NOT LIKE'
  | 'IS NULL'
  | 'IS NOT NULL'
  | 'BETWEEN';

type CTE = {
  name: string;
  query: Query | string;
};

type JoinType = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS';

type WhereCondition = {
  condition: string;
  operator: 'AND' | 'OR';
  isGroup?: boolean;
};

type ConditionalCallback = (query: Query) => Query;

export class Expression {
  constructor(public readonly sql: string) {}

  toString(): string {
    return this.sql;
  }
}

export class Query {
  private _select: (string | Expression)[] = [];
  private _from?: string;
  private _where: WhereCondition[] = [];
  private _groupBy: string[] = [];
  private _having: { condition: string; operator: 'AND' | 'OR' }[] = [];
  private _orderBy: {
    column: string | Expression;
    direction: 'ASC' | 'DESC';
  }[] = [];
  private _limit?: number;
  private _offset?: number;
  private _final = false;
  private _settings: Record<string, string> = {};
  private _ctes: CTE[] = [];
  private _joins: {
    type: JoinType;
    table: string;
    condition: string | Expression;
    final?: boolean;
  }[] = [];
  private _skipNext = false;

  constructor(private client: ClickHouseClient) {}

  // Helper to create raw SQL expressions
  expr(sql: string): Expression {
    return new Expression(sql);
  }

  // Select methods
  select(columns: (string | Expression)[]): this {
    if (this._skipNext) return this;
    this._select = columns;
    return this;
  }

  // From methods
  from(table: string, final = false): this {
    this._from = table;
    this._final = final;
    return this;
  }

  // Where methods
  private escapeValue(value: SqlParam): string {
    if (value === null) return 'NULL';
    if (Array.isArray(value)) {
      return `(${value.map((v) => this.escapeValue(v)).join(', ')})`;
    }
    if (value instanceof Date) {
      return escape(value.toISOString().slice(0, 19).replace('T', ' '));
    }
    return escape(value);
  }

  where(
    column: string | Expression,
    operator: Operator,
    value?: SqlParam,
  ): this {
    if (this._skipNext) return this;
    const condition = this.buildCondition(column, operator, value);
    this._where.push({ condition, operator: 'AND' });
    return this;
  }

  public buildCondition(
    column: string | Expression,
    operator: Operator,
    value?: SqlParam,
  ): string {
    const columnStr = column instanceof Expression ? column.toString() : column;

    switch (operator) {
      case 'IS NULL':
        return `${columnStr} IS NULL`;
      case 'IS NOT NULL':
        return `${columnStr} IS NOT NULL`;
      case 'BETWEEN':
        if (Array.isArray(value) && value.length === 2) {
          return `${columnStr} BETWEEN ${this.escapeValue(value[0]!)} AND ${this.escapeValue(value[1]!)}`;
        }
        throw new Error('BETWEEN operator requires an array of two values');
      case 'IN':
      case 'NOT IN':
        if (!Array.isArray(value)) {
          throw new Error(`${operator} operator requires an array value`);
        }
        return `${columnStr} ${operator} ${this.escapeValue(value)}`;
      default:
        return `${columnStr} ${operator} ${this.escapeValue(value!)}`;
    }
  }

  andWhere(
    column: string | Expression,
    operator: Operator,
    value?: SqlParam,
  ): this {
    const condition = this.buildCondition(column, operator, value);
    this._where.push({ condition, operator: 'AND' });
    return this;
  }

  orWhere(
    column: string | Expression,
    operator: Operator,
    value?: SqlParam,
  ): this {
    const condition = this.buildCondition(column, operator, value);
    this._where.push({ condition, operator: 'OR' });
    return this;
  }

  // Group by methods
  groupBy(columns: (string | Expression)[]): this {
    this._groupBy = columns.map((col) =>
      col instanceof Expression ? col.toString() : col,
    );
    return this;
  }

  // Having methods
  having(
    column: string | Expression,
    operator: Operator,
    value: SqlParam,
  ): this {
    const condition = this.buildCondition(column, operator, value);
    this._having.push({ condition, operator: 'AND' });
    return this;
  }

  andHaving(
    column: string | Expression,
    operator: Operator,
    value: SqlParam,
  ): this {
    const condition = this.buildCondition(column, operator, value);
    this._having.push({ condition, operator: 'AND' });
    return this;
  }

  orHaving(
    column: string | Expression,
    operator: Operator,
    value: SqlParam,
  ): this {
    const condition = this.buildCondition(column, operator, value);
    this._having.push({ condition, operator: 'OR' });
    return this;
  }

  // Order by methods
  orderBy(
    column: string | Expression,
    direction: 'ASC' | 'DESC' = 'ASC',
  ): this {
    if (this._skipNext) return this;
    this._orderBy.push({ column, direction });
    return this;
  }

  // Limit and offset
  limit(limit: number, offset?: number): this {
    this._limit = limit;
    if (offset !== undefined) this._offset = offset;
    return this;
  }

  // Settings
  settings(settings: Record<string, string>): this {
    Object.assign(this._settings, settings);
    return this;
  }

  with(name: string, query: Query | string): this {
    this._ctes.push({ name, query });
    return this;
  }

  // Add join methods
  join(table: string, condition: string | Expression, final = false): this {
    return this.joinWithType('INNER', table, condition, final);
  }

  leftJoin(table: string, condition: string | Expression, final = false): this {
    return this.joinWithType('LEFT', table, condition, final);
  }

  rightJoin(
    table: string,
    condition: string | Expression,
    final = false,
  ): this {
    return this.joinWithType('RIGHT', table, condition, final);
  }

  fullJoin(table: string, condition: string | Expression, final = false): this {
    return this.joinWithType('FULL', table, condition, final);
  }

  crossJoin(table: string, final = false): this {
    return this.joinWithType('CROSS', table, '', final);
  }

  private joinWithType(
    type: JoinType,
    table: string,
    condition: string | Expression,
    final = false,
  ): this {
    this._joins.push({ type, table, condition, final });
    return this;
  }

  // Add methods for grouping conditions
  whereGroup(): WhereGroupBuilder {
    return new WhereGroupBuilder(this, 'AND');
  }

  orWhereGroup(): WhereGroupBuilder {
    return new WhereGroupBuilder(this, 'OR');
  }

  // Update buildQuery method's WHERE section
  private buildWhereConditions(conditions: WhereCondition[]): string {
    return conditions
      .map((w, i) => {
        const condition = w.isGroup ? `(${w.condition})` : w.condition;
        return i === 0 ? condition : `${w.operator} ${condition}`;
      })
      .join(' ');
  }

  private buildQuery(): string {
    const parts: string[] = [];

    // Add WITH clause if CTEs exist
    if (this._ctes.length > 0) {
      const cteStatements = this._ctes.map((cte) => {
        const queryStr =
          typeof cte.query === 'string' ? cte.query : cte.query.toSQL();
        return `${cte.name} AS (${queryStr})`;
      });
      parts.push(`WITH ${cteStatements.join(', ')}`);
    }

    // SELECT
    if (this._select.length > 0) {
      parts.push(
        'SELECT',
        this._select
          .map((col) => (col instanceof Expression ? col.toString() : col))
          .join(', '),
      );
    } else {
      parts.push('SELECT *');
    }

    // FROM
    if (this._from) {
      parts.push(`FROM ${this._from}${this._final ? ' FINAL' : ''}`);

      // Add joins
      this._joins.forEach((join) => {
        const finalClause = join.final ? ' FINAL' : '';
        const conditionStr = join.condition
          ? ` ON ${join.condition instanceof Expression ? join.condition.toString() : join.condition}`
          : '';
        parts.push(
          `${join.type} JOIN ${join.table}${finalClause}${conditionStr}`,
        );
      });
    }

    // WHERE
    if (this._where.length > 0) {
      parts.push('WHERE', this.buildWhereConditions(this._where));
    }

    // GROUP BY
    if (this._groupBy.length > 0) {
      parts.push('GROUP BY', this._groupBy.join(', '));
    }

    // HAVING
    if (this._having.length > 0) {
      const conditions = this._having.map((h, i) => {
        return i === 0 ? h.condition : `${h.operator} ${h.condition}`;
      });
      parts.push('HAVING', conditions.join(' '));
    }

    // ORDER BY
    if (this._orderBy.length > 0) {
      const orderBy = this._orderBy.map((o) => {
        const col =
          o.column instanceof Expression ? o.column.toString() : o.column;
        return `${col} ${o.direction}`;
      });
      parts.push('ORDER BY', orderBy.join(', '));
    }

    // LIMIT & OFFSET
    if (this._limit !== undefined) {
      parts.push(`LIMIT ${this._limit}`);
      if (this._offset !== undefined) {
        parts.push(`OFFSET ${this._offset}`);
      }
    }

    // SETTINGS
    if (Object.keys(this._settings).length > 0) {
      const settings = Object.entries(this._settings)
        .map(([key, value]) => `${key} = ${value}`)
        .join(', ');
      parts.push(`SETTINGS ${settings}`);
    }

    return parts.join(' ');
  }

  // Execution methods
  async execute<T = any>(): Promise<T[]> {
    const query = this.buildQuery();
    const result = await this.client.query({
      query,
      format: 'JSONEachRow',
    });
    return result.json<T>();
  }

  // Debug methods
  toSQL(): string {
    return this.buildQuery();
  }

  // Add method to add where conditions (for internal use)
  _addWhereCondition(condition: WhereCondition): this {
    this._where.push(condition);
    return this;
  }

  if(condition: boolean): this {
    this._skipNext = !condition;
    return this;
  }

  endIf(): this {
    this._skipNext = false;
    return this;
  }

  // Add method for callback-style conditionals
  when(condition: boolean, callback: ConditionalCallback): this {
    if (condition) {
      callback(this);
    }
    return this;
  }
}

// Add this new class for building where groups
export class WhereGroupBuilder {
  private conditions: WhereCondition[] = [];

  constructor(
    private query: Query,
    private groupOperator: 'AND' | 'OR',
  ) {}

  where(
    column: string | Expression,
    operator: Operator,
    value?: SqlParam,
  ): this {
    const condition = this.query.buildCondition(column, operator, value);
    this.conditions.push({ condition, operator: 'AND' });
    return this;
  }

  andWhere(
    column: string | Expression,
    operator: Operator,
    value?: SqlParam,
  ): this {
    const condition = this.query.buildCondition(column, operator, value);
    this.conditions.push({ condition, operator: 'AND' });
    return this;
  }

  orWhere(
    column: string | Expression,
    operator: Operator,
    value?: SqlParam,
  ): this {
    const condition = this.query.buildCondition(column, operator, value);
    this.conditions.push({ condition, operator: 'OR' });
    return this;
  }

  end(): Query {
    const groupCondition = this.conditions
      .map((c, i) => (i === 0 ? c.condition : `${c.operator} ${c.condition}`))
      .join(' ');

    this.query._addWhereCondition({
      condition: groupCondition,
      operator: this.groupOperator,
      isGroup: true,
    });

    return this.query;
  }
}

// Helper function to create a new query
export function createQuery(client: ClickHouseClient): Query {
  return new Query(client);
}

// Export types
export type { SqlValue, SqlParam, Operator };

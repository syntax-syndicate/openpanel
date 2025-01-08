import { flatten, map, pipe, prop, range, sort, uniq } from 'ramda';
import { escape } from 'sqlstring';
import { z } from 'zod';

import {
  TABLE_NAMES,
  ch,
  chQuery,
  createSqlBuilder,
  db,
  fn,
  getSelectPropertyKey,
  toDate,
} from '@openpanel/db';
import {
  zChartInput,
  zCriteria,
  zRange,
  zTimeInterval,
} from '@openpanel/validation';

import { round } from '@openpanel/common';
import { createQuery } from '@openpanel/db';
import {
  differenceInDays,
  differenceInMonths,
  differenceInWeeks,
  formatISO,
} from 'date-fns';
import { getProjectAccessCached } from '../access';
import { TRPCAccessError } from '../errors';
import { createTRPCRouter, protectedProcedure, publicProcedure } from '../trpc';
import {
  getChart,
  getChartPrevStartEndDate,
  getChartStartEndDate,
  getFunnelData,
} from './chart.helpers';

function utc(date: string | Date) {
  if (typeof date === 'string') {
    return date.replace('T', ' ').slice(0, 19);
  }
  return formatISO(date).replace('T', ' ').slice(0, 19);
}

export const chartRouter = createTRPCRouter({
  events: protectedProcedure
    .input(
      z.object({
        projectId: z.string(),
      }),
    )
    .query(async ({ input: { projectId } }) => {
      const events = await createQuery(ch)
        .select([fn.distinct('name')])
        .from(TABLE_NAMES.event_names_mv)
        .where('project_id', '=', projectId)
        .execute<{ name: string }>();

      return [
        {
          name: '*',
        },
        ...events,
      ];
    }),

  properties: protectedProcedure
    .input(
      z.object({
        event: z.string().optional(),
        projectId: z.string(),
      }),
    )
    .query(async ({ input: { projectId, event } }) => {
      const query = createQuery(ch)
        .select([
          fn.distinct('property_key'),
          fn.max('created_at', 'created_at'),
        ])
        .from(TABLE_NAMES.event_property_values_mv)
        .where('project_id', '=', projectId)
        .when(!!event && event !== '*', (q) => q.andWhere('name', '=', event))
        .groupBy(['property_key'])
        .orderBy('created_at', 'DESC');

      const res = await query.execute<{
        property_key: string;
        created_at: string;
      }>();

      const properties = res
        .map((item) => item.property_key)
        .map((item) => item.replace(/\.([0-9]+)\./g, '.*.'))
        .map((item) => item.replace(/\.([0-9]+)/g, '[*]'))
        .map((item) => `properties.${item}`);

      if (event === '*') {
        properties.push('name');
      }

      properties.push(
        'has_profile',
        'path',
        'origin',
        'referrer',
        'referrer_name',
        'duration',
        'created_at',
        'country',
        'city',
        'region',
        'os',
        'os_version',
        'browser',
        'browser_version',
        'device',
        'brand',
        'model',
      );

      return pipe(
        sort<string>((a, b) => a.length - b.length),
        uniq,
      )(properties);
    }),

  values: protectedProcedure
    .input(
      z.object({
        event: z.string(),
        property: z.string(),
        projectId: z.string(),
      }),
    )
    .query(async ({ input: { event, property, projectId, ...input } }) => {
      if (property === 'has_profile') {
        return {
          values: ['true', 'false'],
        };
      }

      const values: string[] = [];

      if (property.startsWith('properties.')) {
        const propertyKey = property.replace(/^properties\./, '');

        const res = await createQuery(ch)
          .select(['distinct property_value', 'max(created_at) as created_at'])
          .from(TABLE_NAMES.event_property_values_mv)
          .where('project_id', '=', projectId)
          .andWhere('property_key', '=', propertyKey)
          .when(!!event && event !== '*', (q) => q.andWhere('name', '=', event))
          .groupBy(['property_value'])
          .orderBy('created_at', 'DESC')
          .execute<{
            property_value: string;
            created_at: string;
          }>();

        values.push(...res.map((e) => e.property_value));
      } else {
        const events = await createQuery(ch)
          .select([`distinct ${getSelectPropertyKey(property)} as values`])
          .from(TABLE_NAMES.events)
          .where('project_id', '=', projectId)
          .when(event !== '*', (q) => q.andWhere('name', '=', event))
          .andWhere(fn.dateDiff('month', 'created_at', fn.now()), '<', 6)
          .orderBy('created_at', 'DESC')
          .limit(100_000)
          .execute<{ values: string[] }>();

        values.push(
          ...pipe(
            (data: typeof events) => map(prop('values'), data),
            flatten,
            uniq,
            sort((a, b) => a.length - b.length),
          )(events),
        );
      }

      return {
        values,
      };
    }),

  funnel: protectedProcedure.input(zChartInput).query(async ({ input }) => {
    const currentPeriod = getChartStartEndDate(input);
    const previousPeriod = getChartPrevStartEndDate({
      range: input.range,
      ...currentPeriod,
    });

    const [current, previous] = await Promise.all([
      getFunnelData({ ...input, ...currentPeriod }),
      input.previous
        ? getFunnelData({ ...input, ...previousPeriod })
        : Promise.resolve(null),
    ]);

    return {
      current,
      previous,
    };
  }),

  chart: publicProcedure.input(zChartInput).query(async ({ input, ctx }) => {
    if (ctx.session.userId) {
      const access = await getProjectAccessCached({
        projectId: input.projectId,
        userId: ctx.session.userId,
      });
      if (!access) {
        const share = await db.shareOverview.findFirst({
          where: {
            projectId: input.projectId,
          },
        });

        if (!share) {
          throw TRPCAccessError('You do not have access to this project');
        }
      }
    } else {
      const share = await db.shareOverview.findFirst({
        where: {
          projectId: input.projectId,
        },
      });

      if (!share) {
        throw TRPCAccessError('You do not have access to this project');
      }
    }

    return getChart(input);
  }),
  cohort: protectedProcedure
    .input(
      z.object({
        projectId: z.string(),
        firstEvent: z.array(z.string()).min(1),
        secondEvent: z.array(z.string()).min(1),
        criteria: zCriteria.default('on_or_after'),
        startDate: z.string().nullish(),
        endDate: z.string().nullish(),
        interval: zTimeInterval.default('day'),
        range: zRange,
      }),
    )
    .query(async ({ input }) => {
      const { projectId, firstEvent, secondEvent } = input;
      const dates = getChartStartEndDate(input);
      const diffInterval = {
        minute: () => differenceInDays(dates.endDate, dates.startDate),
        hour: () => differenceInDays(dates.endDate, dates.startDate),
        day: () => differenceInDays(dates.endDate, dates.startDate),
        week: () => differenceInWeeks(dates.endDate, dates.startDate),
        month: () => differenceInMonths(dates.endDate, dates.startDate),
      }[input.interval]();
      const sqlInterval = {
        minute: 'DAY',
        hour: 'DAY',
        day: 'DAY',
        week: 'WEEK',
        month: 'MONTH',
      }[input.interval];

      const sqlToStartOf = {
        minute: 'toDate',
        hour: 'toDate',
        day: 'toDate',
        week: 'toStartOfWeek',
        month: 'toStartOfMonth',
      }[input.interval];

      const countCriteria = input.criteria === 'on_or_after' ? '>=' : '=';

      const usersSelect = range(0, diffInterval + 1)
        .map(
          (index) =>
            `groupUniqArrayIf(profile_id, x_after_cohort ${countCriteria} ${index}) AS interval_${index}_users`,
        )
        .join(',\n');

      const countsSelect = range(0, diffInterval + 1)
        .map(
          (index) =>
            `length(interval_${index}_users) AS interval_${index}_user_count`,
        )
        .join(',\n');

      const whereEventNameIs = (event: string[]) => {
        if (event.length === 1) {
          return `name = ${escape(event[0])}`;
        }
        return `name IN (${event.map((e) => escape(e)).join(',')})`;
      };

      // const dropoffsSelect = range(1, diffInterval + 1)
      //   .map(
      //     (index) =>
      //       `arrayFilter(x -> NOT has(interval_${index}_users, x), interval_${index - 1}_users) AS interval_${index}_dropoffs`,
      //   )
      //   .join(',\n');

      // const dropoffCountsSelect = range(1, diffInterval + 1)
      //   .map(
      //     (index) =>
      //       `length(interval_${index}_dropoffs) AS interval_${index}_dropoff_count`,
      //   )
      //   .join(',\n');

      // SELECT
      //     project_id,
      //     profile_id AS userID,
      //     name,
      //     toDate(created_at) AS cohort_interval
      // FROM events_v2
      // WHERE profile_id != device_id
      //   AND ${whereEventNameIs(firstEvent)}
      //       AND project_id = ${escape(projectId)}
      //       AND created_at BETWEEN toDate('${utc(dates.startDate)}') AND toDate('${utc(dates.endDate)}')
      // GROUP BY project_id, name, cohort_interval, userID

      const cohortQuery = `
        WITH 
        cohort_users AS (
          SELECT
            profile_id AS userID,
            project_id,
            ${sqlToStartOf}(created_at) AS cohort_interval
          FROM ${TABLE_NAMES.cohort_events_mv}
          WHERE ${whereEventNameIs(firstEvent)}
            AND project_id = ${escape(projectId)}
            AND created_at BETWEEN toDate('${utc(dates.startDate)}') AND toDate('${utc(dates.endDate)}')
        ),
        retention_matrix AS (
          SELECT
            c.cohort_interval,
            e.profile_id,
            dateDiff('${sqlInterval}', c.cohort_interval, ${sqlToStartOf}(e.created_at)) AS x_after_cohort
          FROM cohort_users AS c
          INNER JOIN ${TABLE_NAMES.cohort_events_mv} AS e ON c.userID = e.profile_id
          WHERE (${whereEventNameIs(secondEvent)}) AND (e.project_id = ${escape(projectId)}) 
          AND ((e.created_at >= c.cohort_interval) AND (e.created_at <= (c.cohort_interval + INTERVAL ${diffInterval} ${sqlInterval})))
        ),
        interval_users AS (
          SELECT
            cohort_interval,
            ${usersSelect}
          FROM retention_matrix
          GROUP BY cohort_interval
        ),
        cohort_sizes AS (
          SELECT
            cohort_interval,
            COUNT(DISTINCT userID) AS total_first_event_count
          FROM cohort_users
          GROUP BY cohort_interval
        )
        SELECT
          cohort_interval,
          cohort_sizes.total_first_event_count,
          ${countsSelect}
        FROM interval_users
        LEFT JOIN cohort_sizes AS cs ON cohort_interval = cs.cohort_interval
        ORDER BY cohort_interval ASC
      `;

      const cohortData = await chQuery<{
        cohort_interval: string;
        total_first_event_count: number;
        [key: string]: any;
      }>(cohortQuery);

      return processCohortData(cohortData, diffInterval);
    }),
});

function processCohortData(
  data: Array<{
    cohort_interval: string;
    total_first_event_count: number;
    [key: string]: any;
  }>,
  diffInterval: number,
) {
  if (data.length === 0) {
    return [];
  }

  const processed = data.map((row) => {
    const sum = row.total_first_event_count;
    const values = range(0, diffInterval + 1).map(
      (index) => (row[`interval_${index}_user_count`] || 0) as number,
    );

    return {
      cohort_interval: row.cohort_interval,
      sum,
      values: values,
      percentages: values.map((value) =>
        sum > 0 ? round((value / sum) * 100, 2) : 0,
      ),
    };
  });

  const averageData: {
    totalSum: number;
    values: Array<{ sum: number; weightedSum: number }>;
    percentages: Array<{ sum: number; weightedSum: number }>;
  } = {
    totalSum: 0,
    values: range(0, diffInterval + 1).map(() => ({ sum: 0, weightedSum: 0 })),
    percentages: range(0, diffInterval + 1).map(() => ({
      sum: 0,
      weightedSum: 0,
    })),
  };

  // Aggregate data for weighted averages, excluding zeros
  processed.forEach((row) => {
    averageData.totalSum += row.sum;
    row.values.forEach((value, index) => {
      if (value !== 0) {
        averageData.values[index]!.sum += row.sum;
        averageData.values[index]!.weightedSum += value * row.sum;
      }
    });
    row.percentages.forEach((percentage, index) => {
      if (percentage !== 0) {
        averageData.percentages[index]!.sum += row.sum;
        averageData.percentages[index]!.weightedSum += percentage * row.sum;
      }
    });
  });

  // Calculate weighted average values, excluding zeros
  const averageRow = {
    cohort_interval: 'Weighted Average',
    sum: round(averageData.totalSum / processed.length, 0),
    percentages: averageData.percentages.map(({ sum, weightedSum }) =>
      sum > 0 ? round(weightedSum / sum, 2) : 0,
    ),
    values: averageData.values.map(({ sum, weightedSum }) =>
      sum > 0 ? round(weightedSum / sum, 0) : 0,
    ),
  };

  return [averageRow, ...processed];
}

import pMap from 'p-map';
import { z } from 'zod/v4';
import { ErrorCategory, ErrorDomain, MastraError } from '../../error';
import type { Mastra } from '../../mastra';
import { validateAndSaveScore } from '../../mastra/hooks';
import { getEntityTypeForSpan } from '../../observability';
import type { SpanRecord, TraceRecord, MastraStorage } from '../../storage';
import { createStep, createWorkflow } from '../../workflows/evented';
import type { MastraScorer, ScorerRun } from '../base';
import type { ScoreRowData } from '../types';
import { transformTraceToScorerInputAndOutput } from './utils';

const getTraceStep = createStep({
  id: '__process-trace-scoring',
  inputSchema: z.object({
    targets: z.array(
      z.object({
        traceId: z.string(),
        spanId: z.string().optional(),
      }),
    ),
    scorerId: z.string(),
  }),
  outputSchema: z.any(),
  execute: async ({ inputData, mastra }) => {
    const logger = mastra.getLogger();
    if (!logger) {
      console.warn(
        '[scoreTracesWorkflow] Logger not initialized: no debug or error logs will be recorded for scoring traces.',
      );
    }

    const storage = mastra.getStorage();
    if (!storage) {
      const mastraError = new MastraError({
        id: 'MASTRA_STORAGE_NOT_FOUND_FOR_TRACE_SCORING',
        domain: ErrorDomain.STORAGE,
        category: ErrorCategory.SYSTEM,
        text: 'Storage not found for trace scoring',
        details: {
          scorerId: inputData.scorerId,
        },
      });
      logger?.error(mastraError.toString());
      logger?.trackException(mastraError);
      return;
    }

    let scorer: MastraScorer | undefined;
    try {
      scorer = mastra.getScorerById(inputData.scorerId);
    } catch (error) {
      const mastraError = new MastraError(
        {
          id: 'MASTRA_SCORER_NOT_FOUND_FOR_TRACE_SCORING',
          domain: ErrorDomain.SCORER,
          category: ErrorCategory.SYSTEM,
          text: `Scorer not found for trace scoring`,
          details: {
            scorerId: inputData.scorerId,
          },
        },
        error,
      );
      logger?.error(mastraError.toString());
      logger?.trackException(mastraError);
      return;
    }

    await pMap(
      inputData.targets,
      async target => {
        try {
          await runScorerOnTarget({ storage, scorer, target });
        } catch (error) {
          const mastraError = new MastraError(
            {
              id: 'MASTRA_SCORER_FAILED_TO_RUN_SCORER_ON_TRACE',
              domain: ErrorDomain.SCORER,
              category: ErrorCategory.SYSTEM,
              details: {
                scorerId: scorer.id,
                spanId: target.spanId || '',
                traceId: target.traceId,
              },
            },
            error,
          );
          logger?.error(mastraError.toString());
          logger?.trackException(mastraError);
        }
      },
      { concurrency: 3 },
    );
  },
});

export async function runScorerOnTarget({
  storage,
  scorer,
  target,
}: {
  storage: MastraStorage;
  scorer: MastraScorer;
  target: { traceId: string; spanId?: string };
}) {
  // TODO: add storage api to get a single span
  const observabilityStore = await storage.getStore('observability');
  if (!observabilityStore) {
    throw new MastraError({
      id: 'MASTRA_OBSERVABILITY_STORAGE_NOT_AVAILABLE',
      domain: ErrorDomain.STORAGE,
      category: ErrorCategory.SYSTEM,
      text: 'Observability storage domain is not available',
    });
  }
  const trace = await observabilityStore.getTrace({ traceId: target.traceId });
  if (!trace) {
    throw new Error(`Trace not found for scoring, traceId: ${target.traceId}`);
  }

  let span: SpanRecord | undefined;
  if (target.spanId) {
    span = trace.spans.find(span => span.spanId === target.spanId);
  } else {
    span = trace.spans.find(span => span.parentSpanId === null);
  }

  if (!span) {
    throw new Error(
      `Span not found for scoring, traceId: ${target.traceId}, spanId: ${target.spanId ?? 'Not provided'}`,
    );
  }

  const scorerRun = buildScorerRun({
    scorerType: scorer.type === 'agent' ? 'agent' : undefined,
    trace,
    targetSpan: span,
  });
  const result = await scorer.run({
    ...scorerRun,
    scoreSource: 'test',
    evaluationMode: 'trace',
    targetScope: target.spanId ? 'span' : 'trace',
    targetEntityType: getEntityTypeForSpan(span),
    targetTraceId: target.traceId,
    targetSpanId: target.spanId,
  });

  const scorerResult = {
    ...result,
    scorer: {
      id: scorer.id,
      name: scorer.name || scorer.id,
      description: scorer.description,
      hasJudge: !!scorer.judge,
    },
    traceId: target.traceId,
    spanId: span.spanId,
    entityId: span.entityId || span.entityName || 'unknown',
    entityType: span.spanType,
    entity: { traceId: span.traceId, spanId: span.spanId },
    source: 'TEST',
    scorerId: scorer.id,
  };

  // Legacy score-store emission. This path is being deprecated.
  const savedScoreRecord = await validateAndSaveScore(storage, scorerResult);

  await attachScoreToSpan({
    storage,
    span,
    scoreRecord: savedScoreRecord,
    // The future path above emits the score event; this legacy bridge now only
    // keeps span links up to date during the transition.
    skipCreateScore: true,
  });
}

function buildScorerRun({
  scorerType,
  trace,
  targetSpan,
}: {
  scorerType?: string;
  trace: TraceRecord;
  targetSpan: SpanRecord;
}): ScorerRun {
  if (scorerType === 'agent') {
    const { input, output } = transformTraceToScorerInputAndOutput(trace);
    return { input, output };
  }
  return { input: targetSpan.input, output: targetSpan.output };
}

/**
 * Writes the score to the observability domain via two paths, each wrapped in
 * a try/catch so that one failing doesn't block the other:
 *
 * 1. **updateSpan** — attaches a score link to the span record. Works for
 *    traditional CRUD stores (libsql, pg, etc.) but fails for event-sourced /
 *    append-only stores (DuckDB) that don't support arbitrary span updates.
 *
 * 2. **createScore** — inserts a row into the observability scores table so
 *    it can be queried via listScores() with traceId/spanId filters. Works for
 *    stores that implement the new observability scores domain.
 *
 * TODO: Once all stores implement createScore and all consumers migrate to
 * the observability scores domain, remove the updateSpan path (and the old
 * ScoresStorage.saveScore() call in validateAndSaveScore above).
 */
async function attachScoreToSpan({
  storage,
  span,
  scoreRecord,
  skipCreateScore,
}: {
  storage: MastraStorage;
  span: SpanRecord;
  scoreRecord: ScoreRowData;
  skipCreateScore?: boolean;
}) {
  const observabilityStore = await storage.getStore('observability');
  if (!observabilityStore) {
    throw new MastraError({
      id: 'MASTRA_OBSERVABILITY_STORAGE_NOT_AVAILABLE',
      domain: ErrorDomain.STORAGE,
      category: ErrorCategory.SYSTEM,
      text: 'Observability storage domain is not available',
    });
  }

  // Path 1: Legacy — attach score link to span (fails silently on append-only stores)
  try {
    const existingLinks = span.links || [];
    const link = {
      type: 'score',
      scoreId: scoreRecord.id,
      scorerId: scoreRecord.scorerId ?? scoreRecord.scorer?.id,
      score: scoreRecord.score,
      createdAt: scoreRecord.createdAt,
    };
    await observabilityStore.updateSpan({
      spanId: span.spanId,
      traceId: span.traceId,
      updates: { links: [...existingLinks, link] },
    });
  } catch {
    // Expected for event-sourced stores (e.g. DuckDB) that don't support updateSpan
  }

  if (!skipCreateScore) {
    // Path 2: New — write to observability scores table (fails silently on stores that don't implement it yet)
    try {
      await observabilityStore.createScore({
        score: {
          timestamp: scoreRecord.createdAt ? new Date(scoreRecord.createdAt) : new Date(),
          traceId: span.traceId,
          spanId: span.spanId,
          scorerId: scoreRecord.scorerId ?? (scoreRecord.scorer?.id as string),
          score: scoreRecord.score,
          reason: scoreRecord.reason ?? null,
          experimentId: null,
          scoreTraceId: null,
          metadata: scoreRecord.metadata ?? null,
        },
      });
    } catch {
      // Expected for stores that haven't implemented observability createScore yet
    }
  }
}

export const scoreTracesWorkflow = createWorkflow({
  id: '__batch-scoring-traces',
  inputSchema: z.object({
    targets: z.array(
      z.object({
        traceId: z.string(),
        spanId: z.string().optional(),
      }),
    ),
    scorerId: z.string(),
  }),
  outputSchema: z.any(),
  steps: [getTraceStep],
  options: {
    validateInputs: false,
  },
});

scoreTracesWorkflow.then(getTraceStep).commit();

import { ErrorCategory, ErrorDomain, MastraError } from '../error';
import { saveScorePayloadSchema } from '../evals';
import type { ScoringHookInput, ScoreRowData } from '../evals/types';
import type { Mastra } from '../mastra';
import { EntityType } from '../observability';
import type { MastraStorage } from '../storage';

function toScorerTargetEntityType(entityType: string): EntityType | undefined {
  switch (entityType) {
    case 'AGENT':
      return EntityType.AGENT;
    case 'WORKFLOW':
      return EntityType.WORKFLOW_RUN;
    default:
      return undefined;
  }
}

export function createOnScorerHook(mastra: Mastra) {
  return async (hookData: ScoringHookInput) => {
    const storage = mastra.getStorage();

    if (!storage) {
      mastra.getLogger()?.warn('Storage not found, skipping score validation and saving');
      return;
    }

    const entityId = hookData.entity.id as string;
    const entityType = hookData.entityType;
    const scorer = hookData.scorer;
    const scorerId = scorer.id as string;

    if (!scorerId) {
      mastra.getLogger()?.warn('Scorer ID not found, skipping score validation and saving');
      return;
    }

    try {
      const scorerToUse = await findScorer(mastra, entityId, entityType, scorerId);

      if (!scorerToUse) {
        throw new MastraError({
          id: 'MASTRA_SCORER_NOT_FOUND',
          domain: ErrorDomain.MASTRA,
          category: ErrorCategory.USER,
          text: `Scorer with ID ${scorerId} not found`,
        });
      }

      let input = hookData.input;
      let output = hookData.output;

      const hookDataWithoutObservability = { ...hookData } as Record<string, unknown>;
      delete hookDataWithoutObservability.tracing;
      delete hookDataWithoutObservability.tracingContext;
      delete hookDataWithoutObservability.loggerVNext;
      delete hookDataWithoutObservability.metrics;

      const { structuredOutput, ...rest } = hookDataWithoutObservability as ScoringHookInput;
      const currentSpan = hookData.tracingContext?.currentSpan;
      const traceId = currentSpan?.isValid ? currentSpan.traceId : undefined;
      const runResult = (await scorerToUse.scorer.run({
        ...rest,
        input,
        output,
        scoreSource: 'live',
        evaluationMode: 'live',
        targetScope: 'trace',
        targetEntityType: toScorerTargetEntityType(entityType),
        targetTraceId: traceId,
        tracingMetadata: {
          entityId,
        },
      } as any)) as Record<string, unknown>;
      const score = typeof runResult.score === 'number' ? runResult.score : undefined;
      const reason = typeof runResult.reason === 'string' ? runResult.reason : undefined;

      const payload = {
        ...rest,
        ...runResult,
        score,
        reason,
        entityId,
        scorerId: scorerId,
        traceId,
        scorer: {
          ...rest.scorer,
          hasJudge: !!scorerToUse.scorer.judge,
        },
        metadata: {
          structuredOutput: !!structuredOutput,
        },
      };
      // Legacy score-store emission. This path is being deprecated.
      await validateAndSaveScore(storage, payload);
    } catch (error) {
      const mastraError = new MastraError(
        {
          id: 'MASTRA_SCORER_FAILED_TO_RUN_HOOK',
          domain: ErrorDomain.SCORER,
          category: ErrorCategory.USER,
          details: {
            scorerId,
            entityId,
            entityType,
          },
        },
        error,
      );

      mastra.getLogger()?.trackException(mastraError);
      mastra.getLogger()?.error(mastraError.toString());
    }
  };
}

/**
 * @deprecated Legacy scores-store path. New score emission should use `mastra.observability.addScore()`.
 */
export async function validateAndSaveScore(storage: MastraStorage, payload: unknown): Promise<ScoreRowData> {
  const scoresStore = await storage.getStore('scores');
  if (!scoresStore) {
    throw new MastraError({
      id: 'MASTRA_SCORES_STORAGE_NOT_AVAILABLE',
      domain: ErrorDomain.STORAGE,
      category: ErrorCategory.SYSTEM,
      text: 'Scores storage domain is not available',
    });
  }
  const payloadToSave = saveScorePayloadSchema.parse(payload);
  const result = await scoresStore.saveScore(payloadToSave);
  return result.score;
}

async function findScorer(mastra: Mastra, entityId: string, entityType: string, scorerId: string) {
  let scorerToUse;
  if (entityType === 'AGENT') {
    // Try code-defined agents first
    try {
      const agent = mastra.getAgentById(entityId);
      const scorers = await agent.listScorers();
      for (const [_, scorer] of Object.entries(scorers)) {
        if (scorer.scorer.id === scorerId) {
          scorerToUse = scorer;
          break;
        }
      }
    } catch {
      // Agent not found in code-defined agents, try stored agents via editor
      try {
        const storedAgent = (await mastra.getEditor()?.agent.getById(entityId)) ?? null;
        if (storedAgent) {
          const scorers = await storedAgent.listScorers();
          for (const [_, scorer] of Object.entries(scorers) as [string, any][]) {
            if (scorer.scorer.id === scorerId) {
              scorerToUse = scorer;
              break;
            }
          }
        }
      } catch {
        // Stored agent also not found, will fall back to mastra-registered scorer
      }
    }
  } else if (entityType === 'WORKFLOW') {
    const scorers = await mastra.getWorkflowById(entityId).listScorers();
    for (const [_, scorer] of Object.entries(scorers)) {
      if (scorer.scorer.id === scorerId) {
        scorerToUse = scorer;
        break;
      }
    }
  }

  // Fallback to mastra-registered scorer
  if (!scorerToUse) {
    const mastraRegisteredScorer = mastra.getScorerById(scorerId);
    scorerToUse = mastraRegisteredScorer ? { scorer: mastraRegisteredScorer } : undefined;
  }

  return scorerToUse;
}

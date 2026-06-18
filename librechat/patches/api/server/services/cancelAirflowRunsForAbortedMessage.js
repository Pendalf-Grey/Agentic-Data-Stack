const { logger } = require('@librechat/data-schemas');

const AIRFLOW_ABORT_ENABLED = String(
  process.env.LIBRECHAT_ABORT_AIRFLOW_CANCEL_ENABLED ?? 'true',
).toLowerCase() !== 'false';
const AIRFLOW_ABORT_BASE_URL = (
  process.env.AIRFLOW_API_BASE_URL ||
  process.env.AIRFLOW_BASE_URL ||
  'http://airflow-webserver:8080'
).replace(/\/$/, '');
const AIRFLOW_ABORT_USERNAME = process.env.AIRFLOW_USERNAME || process.env.AIRFLOW_ADMIN_USER || 'admin';
const AIRFLOW_ABORT_PASSWORD =
  process.env.AIRFLOW_PASSWORD || process.env.AIRFLOW_ADMIN_PASSWORD || 'admin';
const AIRFLOW_ABORT_DAG_ID =
  process.env.ADS_LLM_LOG_REFINEMENT_DAG_ID ||
  process.env.AIRFLOW_LLM_SQL_REFINEMENT_DAG_ID ||
  'llm_guided_log_sql_refinement';
const AIRFLOW_ABORT_LOOKBACK_SECONDS = Number(
  process.env.LIBRECHAT_ABORT_AIRFLOW_LOOKBACK_SECONDS || 180,
);

function normalizeQuestionText(value) {
  return String(value || '').trim().replace(/\s+/g, ' ').toLowerCase();
}

function isLikelySameQuestion(userText, airflowQuestion) {
  const userQuestion = normalizeQuestionText(userText);
  const dagQuestion = normalizeQuestionText(airflowQuestion);
  if (!userQuestion || !dagQuestion) {
    return false;
  }
  if (
    userQuestion === dagQuestion ||
    userQuestion.includes(dagQuestion) ||
    dagQuestion.includes(userQuestion)
  ) {
    return true;
  }
  return userQuestion.slice(0, 48) === dagQuestion.slice(0, 48);
}

async function cancelAirflowRunsForAbortedMessage(jobData, context = 'abortMessage') {
  if (!AIRFLOW_ABORT_ENABLED || !jobData?.userMessage?.text) {
    return;
  }

  try {
    const auth = Buffer.from(`${AIRFLOW_ABORT_USERNAME}:${AIRFLOW_ABORT_PASSWORD}`).toString('base64');
    const listUrl = new URL(
      `${AIRFLOW_ABORT_BASE_URL}/api/v1/dags/${encodeURIComponent(AIRFLOW_ABORT_DAG_ID)}/dagRuns`,
    );
    listUrl.searchParams.set('state', 'running');
    listUrl.searchParams.set('order_by', '-start_date');
    listUrl.searchParams.set('limit', '25');

    const listResponse = await fetch(listUrl, {
      headers: {
        Accept: 'application/json',
        Authorization: `Basic ${auth}`,
      },
    });
    if (!listResponse.ok) {
      logger.warn(`[${context}] Airflow running DagRun lookup failed: ${listResponse.status}`);
      return;
    }

    const data = await listResponse.json();
    const runs = Array.isArray(data?.dag_runs) ? data.dag_runs : [];
    const now = Date.now();
    const matched = runs.filter((run) => {
      const runId = String(run?.dag_run_id || '');
      if (!runId.startsWith('mcp__ui-')) {
        return false;
      }
      const runTime = Date.parse(run?.start_date || run?.execution_date || run?.logical_date || '');
      const recent =
        Number.isFinite(runTime) && now - runTime <= AIRFLOW_ABORT_LOOKBACK_SECONDS * 1000;
      const sameQuestion = isLikelySameQuestion(jobData.userMessage.text, run?.conf?.question);
      return sameQuestion || (recent && runs.length === 1);
    });

    await Promise.all(
      matched.map(async (run) => {
        const patchUrl = `${AIRFLOW_ABORT_BASE_URL}/api/v1/dags/${encodeURIComponent(
          AIRFLOW_ABORT_DAG_ID,
        )}/dagRuns/${encodeURIComponent(run.dag_run_id)}`;
        const patchResponse = await fetch(patchUrl, {
          method: 'PATCH',
          headers: {
            Accept: 'application/json',
            Authorization: `Basic ${auth}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ state: 'failed' }),
        });
        if (!patchResponse.ok) {
          logger.warn(
            `[${context}] Airflow DagRun cancel failed: ${run.dag_run_id} ${patchResponse.status}`,
          );
          return;
        }
        // Остановка best-effort: при отмене сообщения гасим фоновый DAG,
        // чтобы Airflow не продолжал прямые Kimi-вызовы после ответа LibreChat.
        logger.info(`[${context}] Cancelled Airflow DagRun after LibreChat abort: ${run.dag_run_id}`);
      }),
    );
  } catch (error) {
    logger.warn(`[${context}] Airflow cancellation skipped after error`, error);
  }
}

module.exports = { cancelAirflowRunsForAbortedMessage };

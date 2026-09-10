const fs = require('fs');

function replaceOnce(source, needle, replacement, label) {
  const first = source.indexOf(needle);
  if (first === -1 || source.indexOf(needle, first + needle.length) !== -1) {
    throw new Error(`Unable to apply ADS progress patch: ${label}`);
  }
  return source.slice(0, first) + replacement + source.slice(first + needle.length);
}

const mcpPath = '/app/api/server/services/MCP.js';
let mcpSource = fs.readFileSync(mcpPath, 'utf8');

mcpSource = replaceOnce(
  mcpSource,
  `function createOAuthStart({ flowId, flowManager, callback }) {`,
  `function createMcpProgressEmitter({ res, stepId, toolCall, streamId = null }) {
  return async function (progress) {
    const data = {
      id: stepId,
      delta: {
        type: StepTypes.TOOL_CALLS,
        tool_calls: [
          { ...toolCall, args: '\\n__ADS_PROGRESS__=' + JSON.stringify(progress) + '\\n' },
        ],
      },
    };
    const eventData = { event: GraphEvents.ON_RUN_STEP_DELTA, data };
    if (streamId) {
      await GenerationJobManager.emitChunk(streamId, eventData);
    } else {
      sendEvent(res, eventData);
    }
  };
}

function createOAuthStart({ flowId, flowManager, callback }) {`,
  'MCP progress emitter',
);

const adsWorkflowCancellationHelper = [
  'async function cancelAdsWorkflow(investigationId) {',
  "  const controlToken = process.env.ADS_WORKFLOW_CONTROL_TOKEN;",
  '  if (!controlToken || !investigationId) {',
  '    return;',
  '  }',
  "  const response = await fetch('http://mcp-log-workflow:8000/internal/ads/workflow/cancel', {",
  "    method: 'POST',",
  '    headers: {',
  "      'Content-Type': 'application/json',",
  "      'X-ADS-Workflow-Control-Token': controlToken,",
  '    },',
  '    body: JSON.stringify({ investigation_id: investigationId }),',
  '  });',
  '  if (!response.ok) {',
  "    throw new Error('Workflow cancellation failed with status ' + response.status);",
  '  }',
  '}',
].join('\n');

mcpSource = replaceOnce(
  mcpSource,
  'function createOAuthStart({ flowId, flowManager, callback }) {',
  adsWorkflowCancellationHelper + '\n\nfunction createOAuthStart({ flowId, flowManager, callback }) {',
  'MCP workflow cancellation helper',
);

mcpSource = replaceOnce(
  mcpSource,
  `      const oauthEnd = createOAuthEnd({\n        res,\n        stepId,\n        toolCall,\n        streamId,\n      });`,
`      const oauthEnd = createOAuthEnd({\n        res,\n        stepId,\n        toolCall,\n        streamId,\n      });\n      const progressToken =\n        'ads-progress-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2);\n      const emitProgress = createMcpProgressEmitter({ res, stepId, toolCall, streamId });\n      const onprogress = (notification) => {\n        if (serverName !== 'ads-log-workflow' || toolName !== 'run_log_analysis') {\n          return;\n        }\n        const { progress, total, message } = notification?.params ?? notification ?? {};\n        void emitProgress({ progress, total, message }).catch((error) => {\n          logger.warn(\n            '[MCP][' + serverName + '][' + toolName + '] Progress update failed: ' + error.message,\n          );\n        });\n      };`,
  'MCP progress callback',
);

const progressCallbackNeedle = [
  '      const progressToken =',
  "        'ads-progress-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2);",
  '      const emitProgress = createMcpProgressEmitter({ res, stepId, toolCall, streamId });',
  '      const onprogress = (notification) => {',
  "        if (serverName !== 'ads-log-workflow' || toolName !== 'run_log_analysis') {",
  '          return;',
  '        }',
  '        const { progress, total, message } = notification?.params ?? notification ?? {};',
  '        void emitProgress({ progress, total, message }).catch((error) => {',
  '          logger.warn(',
  "            '[MCP][' + serverName + '][' + toolName + '] Progress update failed: ' + error.message,",
  '          );',
  '        });',
  '      };',
].join('\n');

const progressCallbackReplacement = [
  '      const progressToken =',
  "        'ads-progress-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2);",
  '      const emitProgress = createMcpProgressEmitter({ res, stepId, toolCall, streamId });',
  "      let activeInvestigationId = '';",
  '      const onprogress = (notification) => {',
  "        if (serverName !== 'ads-log-workflow' || toolName !== 'run_log_analysis') {",
  '          return;',
  '        }',
  '        const { progress, total, message } = notification?.params ?? notification ?? {};',
  "        const rawMessage = typeof message === 'string' ? message : '';",
  '        const idMatch = rawMessage.match(/(?:^|\\n)__ADS_INVESTIGATION_ID__=([^\\s]+)/);',
  '        if (idMatch?.[1]) {',
  '          activeInvestigationId = idMatch[1];',
  '          const workflowRegistry = globalThis.__adsActiveWorkflowRegistry ??= new Map();',
  '          const conversationId =',
  '            config?.configurable?.requestBody?.conversationId ??',
  '            config?.metadata?.conversationId ??',
  "            '';",
  '          workflowRegistryKey =',
  "            userId + ':' + (conversationId || config?.metadata?.run_id || activeInvestigationId);",
  '          workflowRegistry.set(workflowRegistryKey, {',
  '            investigationId: activeInvestigationId,',
  '            userId,',
  '            conversationId,',
  '          });',
  '        }',
  '        const displayMessage = rawMessage',
  '          .replace(/(?:^|\\n)__ADS_INVESTIGATION_ID__=[^\\n]*\\n?/, \'\')',
  '          .trim();',
  '        void emitProgress({ progress, total, message: displayMessage }).catch((error) => {',
  '          logger.warn(',
  "            '[MCP][' + serverName + '][' + toolName + '] Progress update failed: ' + error.message,",
  '          );',
  '        });',
  '      };',
].join('\n');

mcpSource = replaceOnce(
  mcpSource,
  progressCallbackNeedle,
  progressCallbackReplacement,
  'MCP progress investigation binding',
);

const abortHandlerNeedle = [
  '      if (derivedSignal) {',
  '        abortHandler = createAbortHandler({ userId, serverName, toolName, flowManager });',
  "        derivedSignal.addEventListener('abort', abortHandler, { once: true });",
  '      }',
].join('\n');

const abortHandlerReplacement = [
  '      if (derivedSignal) {',
  '        const cleanupAbortHandler = createAbortHandler({ userId, serverName, toolName, flowManager });',
  '        abortHandler = () => {',
  '          cleanupAbortHandler();',
  '          if (activeInvestigationId) {',
  '            void cancelAdsWorkflow(activeInvestigationId).catch((error) => {',
  '              logger.warn(',
  "                '[MCP][' + serverName + '][' + toolName + '] Workflow cancellation failed: ' + error.message,",
  '              );',
  '            });',
  '          }',
  '        };',
  "        derivedSignal.addEventListener('abort', abortHandler, { once: true });",
  '      }',
].join('\n');

mcpSource = replaceOnce(
  mcpSource,
  abortHandlerNeedle,
  abortHandlerReplacement,
  'MCP workflow cancellation handler',
);

mcpSource = replaceOnce(
  mcpSource,
  [
    '    /** @type {AbortSignal} */',
    '    let derivedSignal = null;',
  ].join('\n'),
  [
    '    /** @type {AbortSignal} */',
    '    let derivedSignal = null;',
    '    let workflowCloseHandler = null;',
    "    let workflowRegistryKey = '';",
  ].join('\n'),
  'MCP response-close cancellation state',
);

const responseCloseHook = [
  "      if (serverName === 'ads-log-workflow' && toolName === 'run_log_analysis' && typeof res?.once === 'function') {",
  '        workflowCloseHandler = () => {',
  '          if (activeInvestigationId) {',
  '            logger.info(',
  "              '[MCP][' + serverName + '][' + toolName + '] Response closed; cancelling workflow ' + activeInvestigationId,",
  '            );',
  '            void cancelAdsWorkflow(activeInvestigationId).catch((error) => {',
  '              logger.warn(',
  "                '[MCP][' + serverName + '][' + toolName + '] Workflow cancellation failed: ' + error.message,",
  '              );',
  '            });',
  '          }',
  '        };',
  "        res.once('close', workflowCloseHandler);",
  '      }',
].join('\n');

mcpSource = replaceOnce(
  mcpSource,
  abortHandlerReplacement,
  abortHandlerReplacement + '\n\n' + responseCloseHook,
  'MCP response-close cancellation handler',
);

mcpSource = replaceOnce(
  mcpSource,
  [
    '      if (abortHandler && derivedSignal) {',
    "        derivedSignal.removeEventListener('abort', abortHandler);",
    '      }',
  ].join('\n'),
  [
    '      if (abortHandler && derivedSignal) {',
    "        derivedSignal.removeEventListener('abort', abortHandler);",
    '      }',
    "      if (workflowCloseHandler && typeof res?.removeListener === 'function') {",
    "        res.removeListener('close', workflowCloseHandler);",
    '      }',
    '      if (workflowRegistryKey) {',
    '        globalThis.__adsActiveWorkflowRegistry?.delete(workflowRegistryKey);',
    '      }',
  ].join('\n'),
  'MCP workflow registry cleanup',
);

mcpSource = replaceOnce(
  mcpSource,
  `        options: {\n          signal: derivedSignal,\n        },`,
  `        options: {\n          signal: derivedSignal,\n          progressToken,\n          onprogress,\n        },`,
  'MCP progress request options',
);

fs.writeFileSync(mcpPath, mcpSource);

const apiPath = '/app/packages/api/dist/index.js';
let apiSource = fs.readFileSync(apiPath, 'utf8');
apiSource = replaceOnce(
  apiSource,
  `params: {\n                        name: toolName,\n                        arguments: toolArguments,\n                    },\n                }, types_js.CallToolResultSchema,`,
  `params: {\n                        name: toolName,\n                        arguments: toolArguments,\n                        _meta: (options === null || options === void 0 ? void 0 : options.progressToken)\n                            ? { progressToken: options.progressToken }\n                            : undefined,\n                    },\n                }, types_js.CallToolResultSchema,`,
  'MCP progress token',
);
fs.writeFileSync(apiPath, apiSource);

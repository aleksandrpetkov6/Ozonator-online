#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";

const AUTOFIX_TOKEN_BUDGET_MAX = 4800;
const APPROX_CHARS_PER_TOKEN = 3;
const PROMPT_CHAR_BUDGET = Math.floor(AUTOFIX_TOKEN_BUDGET_MAX * APPROX_CHARS_PER_TOKEN * 0.7);

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const key = a.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      out[key] = true;
    } else {
      out[key] = next;
      i++;
    }
  }
  return out;
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function readTextSafe(file) {
  try {
    return fs.readFileSync(file, "utf8");
  } catch {
    return "";
  }
}

function listFilesRecursive(root) {
  const result = [];
  if (!fs.existsSync(root)) return result;

  const walk = (p) => {
    const stat = fs.statSync(p);
    if (stat.isDirectory()) {
      for (const name of fs.readdirSync(p)) {
        walk(path.join(p, name));
      }
    } else if (stat.isFile()) {
      result.push(p);
    }
  };

  walk(root);
  return result;
}

function truncate(s, max) {
  if (!s) return "";
  if (s.length <= max) return s;
  return s.slice(0, max) + `\n\n[TRUNCATED ${s.length - max} chars]`;
}

function collectLogs(logsDir) {
  const files = listFilesRecursive(logsDir).filter((f) => {
    const lower = f.toLowerCase();
    return (
      lower.endsWith(".txt") ||
      lower.endsWith(".log") ||
      lower.endsWith(".md") ||
      lower.endsWith(".out") ||
      lower.endsWith(".err") ||
      lower.includes("log")
    );
  });

  const chunks = [];
  for (const f of files.slice(0, 50)) {
    const content = readTextSafe(f);
    if (!content.trim()) continue;
    chunks.push(`===== FILE: ${f} =====\n${truncate(content, 3000)}`);
  }

  return truncate(chunks.join("\n\n"), 18000);
}

function safeExec(cmd) {
  try {
    return execSync(cmd, {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (e) {
    const stdout = e?.stdout ? String(e.stdout) : "";
    const stderr = e?.stderr ? String(e.stderr) : "";
    return [stdout, stderr].filter(Boolean).join("\n");
  }
}

function estimateTokens(text) {
  if (!text) return 0;
  return Math.ceil(String(text).length / APPROX_CHARS_PER_TOKEN);
}

function fitPromptToBudget({ runId, sha, triggerWorkflow, logsText }) {
  const fallbackLogs = "[logs omitted due to token budget precheck]";
  const baseline = buildPrompt({ runId, sha, triggerWorkflow, logsText: fallbackLogs });
  const baselineTokens = estimateTokens(baseline);

  if (baselineTokens >= AUTOFIX_TOKEN_BUDGET_MAX) {
    throw new Error(
      `AUTOFIX_TOKEN_BUDGET_PRECHECK: baseline prompt ${baselineTokens} exceeds hard limit ${AUTOFIX_TOKEN_BUDGET_MAX}`
    );
  }

  const allowedExtraChars = Math.max(0, PROMPT_CHAR_BUDGET - baseline.length - 200);
  const boundedLogs = logsText ? truncate(logsText, allowedExtraChars) : fallbackLogs;
  const prompt = buildPrompt({
    runId,
    sha,
    triggerWorkflow,
    logsText: boundedLogs || fallbackLogs,
  });
  const promptTokens = estimateTokens(prompt);

  if (promptTokens >= AUTOFIX_TOKEN_BUDGET_MAX) {
    throw new Error(
      `AUTOFIX_TOKEN_BUDGET_PRECHECK: prompt ${promptTokens} exceeds hard limit ${AUTOFIX_TOKEN_BUDGET_MAX}`
    );
  }

  return {
    prompt,
    promptTokens,
    logsWereTrimmed: String(boundedLogs || "").length < String(logsText || "").length,
  };
}

function buildPrompt({ runId, sha, triggerWorkflow, logsText }) {
  const gitStatus = safeExec("git status --short");
  const fileList = safeExec("git ls-files").split("\n").slice(0, 500).join("\n");

  return `
You are AutoFix for CI. Return ONLY a valid unified git patch (no markdown, no explanations).

Goal:
- Generate a patch that fixes the CI failure based on logs.
- Patch must be applicable to the current repository state (HEAD SHA below).
- Patch must be minimal and focused.

Hard rules:
1) Output ONLY the patch text.
2) Do NOT wrap in code fences.
3) Do NOT include prose.
4) If you must edit multiple files, include all changes in one patch.
5) Prefer deterministic fixes and preserve existing behavior unless logs show a clear bug.
6) If exact fix is unclear, make the smallest safe change that unblocks CI.
7) Patch must use proper unified diff format.

Context:
- Trigger workflow: ${triggerWorkflow || ""}
- Failed run id: ${runId || ""}
- Failed SHA: ${sha || ""}

Current git status (for reference):
${truncate(gitStatus, 1500)}

Repository tracked files (first 500):
${truncate(fileList, 4000)}

CI logs:
${logsText || "[no logs found]"}

Now output ONLY the patch.
`.trim();
}

function extractPatch(raw) {
  let s = String(raw || "").trim();

  // Remove markdown fences if model ignored instructions
  s = s.replace(/^```(?:diff|patch|text)?\s*/i, "");
  s = s.replace(/\s*```$/i, "").trim();

  const idxDiff = s.indexOf("diff --git ");
  if (idxDiff >= 0) return s.slice(idxDiff).trim();

  // Sometimes model returns unified diff without "diff --git"
  const idxTriple = s.indexOf("--- ");
  if (idxTriple >= 0) return s.slice(idxTriple).trim();

  return s.trim();
}

function validatePatchText(patch) {
  if (!patch || !patch.trim()) {
    throw new Error("Model returned empty patch");
  }

  const hasUnifiedMarkers =
    patch.includes("diff --git ") ||
    (patch.includes("--- ") && patch.includes("+++ "));

  if (!hasUnifiedMarkers) {
    throw new Error("Response is not a valid unified diff patch");
  }
}

async function callOpenAICompatible({ baseUrl, apiKey, model, prompt, provider }) {
  const url = `${baseUrl.replace(/\/+$/, "")}/chat/completions`;

  const headers = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${apiKey}`,
  };

  // OpenRouter can benefit from extra headers (optional)
  if (provider === "openrouter") {
    headers["HTTP-Referer"] = "https://github.com";
    headers["X-Title"] = "Ozonator AutoFix";
  }

  const body = {
    model,
    temperature: 0,
    messages: [
      {
        role: "system",
        content:
          "You are a CI autofix agent. Output only a valid git patch in unified diff format. No markdown. No explanations.",
      },
      {
        role: "user",
        content: prompt,
      },
    ],
  };

  const res = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  const text = await res.text();

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`Non-JSON response from provider (${res.status}): ${text}`);
  }

  if (!res.ok) {
    throw new Error(`Provider API error ${res.status}: ${JSON.stringify(data)}`);
  }

  const content = data?.choices?.[0]?.message?.content;
  if (!content) {
    throw new Error(`Provider response has no choices[0].message.content: ${JSON.stringify(data)}`);
  }

  return String(content);
}

async function generatePatch(prompt) {
  const provider = (process.env.AUTOFIX_PROVIDER || "groq").toLowerCase();
  const model = process.env.AUTOFIX_MODEL || "openai/gpt-oss-20b";
  const apiKey = process.env.AUTOFIX_API_KEY;

  if (!apiKey) {
    throw new Error("Missing AUTOFIX_API_KEY");
  }

  const baseUrlFromEnv = process.env.AUTOFIX_BASE_URL;

  if (provider === "groq") {
    const baseUrl = baseUrlFromEnv || "https://api.groq.com/openai/v1";
    return callOpenAICompatible({ baseUrl, apiKey, model, prompt, provider });
  }

  if (provider === "openai") {
    const baseUrl = baseUrlFromEnv || "https://api.openai.com/v1";
    return callOpenAICompatible({ baseUrl, apiKey, model, prompt, provider });
  }

  if (provider === "openrouter") {
    const baseUrl = baseUrlFromEnv || "https://openrouter.ai/api/v1";
    return callOpenAICompatible({ baseUrl, apiKey, model, prompt, provider });
  }

  throw new Error(
    `Unsupported AUTOFIX_PROVIDER="${provider}". Supported in this script: groq, openai, openrouter`
  );
}

function shouldKeepExistingPatchOnProviderError(err) {
  const msg = String(err?.stack || err?.message || err || "");
  return (
    msg.includes('tool_use_failed') ||
    msg.includes('Tool choice is none') ||
    msg.includes('failed_generation')
  );
}

function hasUsableExistingLatestPatch() {
  try {
    return fs.existsSync('patches/latest.patch') && fs.statSync('patches/latest.patch').size > 0;
  } catch {
    return false;
  }
}

async function main() {
  const args = parseArgs(process.argv);
  const logsDir = args["logs-dir"] || "_ci_logs";
  const runId = args["run-id"] || "";
  const sha = args["sha"] || "";
  const triggerWorkflow = args["trigger-workflow"] || "";

  ensureDir("patches");

  const logsText = collectLogs(logsDir);
  const { prompt, promptTokens, logsWereTrimmed } = fitPromptToBudget({
    runId,
    sha,
    triggerWorkflow,
    logsText,
  });

  // Debug artifacts (useful when patch generation fails)
  fs.writeFileSync("patches/_autofix_prompt.txt", prompt, "utf8");
  fs.writeFileSync(
    "patches/_autofix_budget.json",
    JSON.stringify(
      {
        tokenBudgetAllowed: AUTOFIX_TOKEN_BUDGET_MAX,
        promptTokensEstimated: promptTokens,
        logsWereTrimmed,
      },
      null,
      2
    ),
    "utf8"
  );
  console.log(
    `[AutoFix] token budget: estimated=${promptTokens}, allowed=${AUTOFIX_TOKEN_BUDGET_MAX}, logsWereTrimmed=${logsWereTrimmed}`
  );

  const raw = await generatePatch(prompt);
  fs.writeFileSync("patches/_autofix_raw_response.txt", raw, "utf8");

  const patch = extractPatch(raw);
  validatePatchText(patch);

  fs.writeFileSync("patches/latest.patch", patch.endsWith("\n") ? patch : patch + "\n", "utf8");
  console.log("[AutoFix] Saved patches/latest.patch");
}

main().catch((err) => {
  const message = String(err?.stack || err?.message || err || "");
  fs.writeFileSync('patches/_autofix_error.txt', message, 'utf8');

  if (shouldKeepExistingPatchOnProviderError(err) && hasUsableExistingLatestPatch()) {
    console.warn('[AutoFix] Non-actionable provider error. Keeping existing patches/latest.patch');
    process.exit(0);
  }

  console.error("[AutoFix] ERROR:", message);
  process.exit(1);
});

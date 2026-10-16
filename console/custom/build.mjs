import { cp, mkdir, readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { StoreProviders } from './model.mjs';

export function patchStoreProviders(source) {
  const original = '{APPLE_APP_STORE:0,GOOGLE_PLAY_STORE:1,HUAWEI_APP_GALLERY:2,FACEBOOK_INSTANT_STORE:3,SAMSUNG_GALAXY_STORE:4}';
  if (source.split(original).length !== 2) throw new Error('Expected exactly one upstream store enum map. Review the new console bundle before upgrading.');
  const extra = Object.entries(StoreProviders).map(([key, value]) => `${key}:${value}`).join(',');
  return source.replace(original, original.slice(0, -1) + ',' + extra + '}');
}

export async function buildConsole(output) {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const source = path.resolve(here, '../ui/dist');
  if (path.resolve(output) === source) throw new Error('Choose a separate output directory to preserve upstream assets.');
  await mkdir(output, { recursive: true });
  await cp(source, output, { recursive: true });
  let html = await readFile(path.join(source, 'index.html'), 'utf8');
  const match = html.match(/src="(\/static\/index-[^"/]+\.js)"/);
  if (!match) throw new Error('Upstream console entry point was not found.');
  const js = patchStoreProviders(await readFile(path.join(source, match[1]), 'utf8'));
  const digest = createHash('sha256').update(js).digest('hex').slice(0, 12);
  const name = `/static/index-cardgame-${digest}.js`;
  await writeFile(path.join(output, name), js);
  await writeFile(path.join(output, name + '.gz'), gzipSync(js));
  html = html.replace(match[1], name);
  html = html.replace('</head>', '  <link rel="stylesheet" href="/custom-console.css">\n    <script type="module" src="/custom-console.mjs"></script>\n  </head>');
  await writeFile(path.join(output, 'index.html'), html);
  await writeFile(path.join(output, 'index.html.gz'), gzipSync(html));
  for (const file of ['custom-console.mjs', 'notification-settings.mjs', 'currency-art.mjs', 'obsolete-items.mjs', 'model.mjs', 'api.mjs', 'resource-picker.mjs', 'popup-tabs.mjs', 'notification-popup.mjs', 'wallet-popup.mjs', 'purchase-panel.mjs', 'custom-console.css']) await cp(path.join(here, file), path.join(output, file));
  await cp(path.join(here, 'currency-art'), path.join(output, 'currency-art'), { recursive: true });
  console.log(`Custom console built: ${output}`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (!process.argv[2]) throw new Error('Usage: node console/custom/build.mjs OUTPUT_DIRECTORY');
  await buildConsole(process.argv[2]);
}

import fs from 'node:fs/promises';
import path from 'node:path';

const htmlPath = path.resolve(process.cwd(), 'dist/renderer/index.html');
const raw = await fs.readFile(htmlPath, 'utf8');
const patched = raw
  .replaceAll('src="/assets/', 'src="./assets/')
  .replaceAll('href="/assets/', 'href="./assets/');

if (patched !== raw) {
  await fs.writeFile(htmlPath, patched, 'utf8');
  console.log(`[fix-renderer-paths] patched asset paths in ${htmlPath}`);
} else {
  console.log(`[fix-renderer-paths] no asset path rewrite needed in ${htmlPath}`);
}

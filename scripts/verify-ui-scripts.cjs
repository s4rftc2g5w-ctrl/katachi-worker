const fs = require('node:fs');
const path = require('node:path');

const files = ['HTML.html', 'STATUS_HTML.html', 'unifiedHTML.html'];
for (const file of files) {
  const html = fs.readFileSync(path.join('structured', 'ui', file), 'utf8');
  const scripts = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)];
  for (const [index, match] of scripts.entries()) {
    try {
      new Function(match[1]);
    } catch (error) {
      throw new Error(`${file} script ${index + 1}: ${error.message}`);
    }
  }
}

console.log('PASS: inline UI scripts have valid JavaScript syntax');

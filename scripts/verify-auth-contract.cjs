const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve('structured');
const worker = fs.readFileSync(path.join(root, 'worker.template.js'), 'utf8');
const legacy = fs.readFileSync(path.join(root, 'ui', 'HTML.html'), 'utf8');
const status = fs.readFileSync(path.join(root, 'ui', 'STATUS_HTML.html'), 'utf8');
const unified = fs.readFileSync(path.join(root, 'ui', 'unifiedHTML.html'), 'utf8');
const allUi = legacy + '\n' + status + '\n' + unified;

if (/\/api\/[^"'\s]*[?&]k=/.test(allUi)) {
  throw new Error('API key must not be placed in an API URL');
}
if (/searchParams\.get\(["']k["']\)/.test(worker)) {
  throw new Error('Worker must not accept API keys from query parameters');
}
if (!worker.includes("request.headers.get('authorization')")) {
  throw new Error('Worker is missing Authorization header authentication');
}

const authCheck = worker.indexOf("requestAccessKey(request) !== env.ACCESS_KEY");
const discoverRoute = worker.indexOf("url.pathname === '/api/bidnote-discover'");
if (authCheck < 0 || discoverRoute < 0 || authCheck > discoverRoute) {
  throw new Error('Bid Note discovery must pass the shared authentication check');
}
if (!unified.includes('"Authorization":"Bearer " + discoverKey')) {
  throw new Error('Unified UI must authenticate Bid Note discovery');
}
if (!legacy.includes('"Authorization": "Bearer " + apiKey()')) {
  throw new Error('Legacy UI must send its API key in the Authorization header');
}
if (!status.includes("'Authorization':'Bearer '+k") || !status.includes('"Authorization":"Bearer "+k')) {
  throw new Error('Status and backup requests must use Authorization headers');
}

console.log('PASS: API keys stay out of URLs and Bid Note discovery requires authentication');

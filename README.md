# katachi Worker

公開中の v1.5.80 を起点に、画面とサーバー処理を分けて管理するための正本です。

## 構成

- `structured/worker.template.js` — Cloudflare Workerの処理。HTML部分はプレースホルダーです。
- `structured/ui/HTML.html` — 旧画面。
- `structured/ui/unifiedHTML.html` — PC・iPhone共通の統合版。
- `structured/ui/STATUS_HTML.html` — 動作状況画面。
- `scripts/build-worker.cjs` — 上記を公開用の単一Workerへまとめます。
- `structured/baseline.json` — 分離元v1.5.80と画面ファイルのハッシュです。

## 確認

```powershell
npm run verify
```

生成物は `build/worker.js` です。生成物やZIPを手で直さず、`structured` のファイルを変更して再生成します。

## 現在地

分離前後で画面内容が同一であることと、生成Workerの構文を確認しています。本番公開、Cloudflare設定、Cron、KVは変更していません。候補台帳v1.5.81は試作であり、この正本にはまだ統合していません。

作業ブランチでは、APIの合言葉をURLの `?k=` に付けず `Authorization: Bearer` ヘッダーで送ります。`/api/bidnote-discover` も同じ認証を必須にしています。本番へはまだ反映していません。

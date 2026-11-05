# 毒と炎のカード決闘

カードで相手の体力を削り合う対戦カードゲーム。CPU戦・ひとつの端末での対戦・オンライン対戦・ランキング・観戦・自作カードに対応。

## ファイル
- `card-duel.html` … ゲーム本体（claude.ai の公開版と同じもの）
- `shim.js` … ブラウザ側で claude.ai の db / room / user と同じ仕組みを用意し、このサーバーにつなぐ
- `server.js` … ページの配信と、対戦・ランキング・自作カード・絵柄のデータを扱う WebSocket サーバー
- `seed/cardart.json` … 最初から入れておくカードの絵柄（`{"カードキー":"data:image/jpeg;base64,…"}`）

## 動かし方
```
npm install
ADMIN_KEY=好きな合言葉 npm start
```
http://localhost:3000 を開く。

## 管理者（絵柄の登録）
`https://サーバーのURL/#admin=ADMIN_KEYの値` を一度開くと、その端末が管理者として記憶され、「絵柄の登録」画面が使える。

## 注意
無料プランのサーバーは再起動するとデータ（プロフィール・レート・自作カード）が消える。絵柄は `seed/cardart.json` から毎回読み込まれる。

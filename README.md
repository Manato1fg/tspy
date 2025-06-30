# TSPY

TSPY（Task Spooler in PYthon）は、シンプルかつ強力なコマンドライン用のジョブ管理ツールです。  
コマンドをキューに追加し、並列実行やGPU/CPU割り当て、優先度制御、一時停止・再開・停止などの機能を備えています。  
機械学習タスクやバッチ処理、自動化スクリプトの管理に最適です。

---

## 主な機能

- コマンドのキューイングと並列実行（`-j`で並列数指定）
- GPU/CPUデバイスの割り当て（`--gpu <番号>`）
- ジョブの優先度設定（`--priority`）
- ジョブの一時停止・再開・停止（`pause`, `resume`, `kill`）
- ジョブの出力・エラーの個別ファイル保存
- JST（日本標準時）でのタイムスタンプ表示

---

## インストール

Python 3.8以降が必要です。

```bash
git clone https://github.com/Manato1fg/tspy.git
cd tspy
```

---

## 使い方

### ジョブの追加

```bash
python tspy.py add "<コマンド>" [--priority 優先度] [--gpu GPU番号] [--cwd 作業ディレクトリ]
```

- 例:  
  - `python tspy.py add "python train.py" --priority 10 --gpu 0`
  - `python tspy.py add "echo Hello"`

### ワーカーの起動（ジョブ実行）

```bash
python tspy.py worker [-j 並列数]
```

- 例:  
  - `python tspy.py worker -j 2`

### ジョブ一覧の表示

```bash
python tspy.py status
```

### ジョブの出力・エラー確認

```bash
python tspy.py output <ジョブID>
python tspy.py error <ジョブID>
```

### ジョブの一時停止・再開・停止

```bash
python tspy.py pause <ジョブID>
python tspy.py resume <ジョブID>
python tspy.py kill <ジョブID>
```

### ジョブの削除

```bash
python tspy.py remove <ジョブID>
python tspy.py remove --all -f  # 全削除（強制）
```

---

## GPU/CPU割り当てについて

- `--gpu`オプションでジョブを特定のGPUに割り当て
- 指定しない場合はCPUジョブとして扱われます
- ワーカーは同じGPUに同時に複数ジョブを割り当てません

---

## ライセンス

MIT

---

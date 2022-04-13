## 仕様メモ


取引所からのレスポンスを受けるときはカスタムデータクラス

変換する場合は pd.DataFrame 化 → pandera validation を通す

DB への永続化は DataFrame に対して実施（SQLAlchemy と pandera のスキーマを合わせておく）

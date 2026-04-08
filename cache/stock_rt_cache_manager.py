"""股票实时行情专用缓存管理器（5 分钟 TTL）"""
import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict
import pandas as pd
from config.settings import CACHE_DB


class StockRtCacheManager:
    """股票实时行情专用缓存管理器

    用途：
    1. 缓存股票实时日线行情数据（Tushare rt_k 或 Akshare 备用）
    2. TTL: 5 分钟（300 秒），适合实时数据的短暂缓存
    3. 支持按股票代码查询缓存

    表结构：
    - stock_rt_snapshot: 股票实时快照表
      - ts_code: 股票代码（主键）
      - name: 股票名称
      - close/open/high/low/pre_close: 价格数据
      - change/pct_chg: 涨跌数据
      - vol/amount: 成交量数据
      - updated_at: 更新时间戳
    """

    # 5 分钟 TTL（秒）
    CACHE_TTL = 300

    def __init__(self, db_path: Path = CACHE_DB):
        """初始化实时行情缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # 启用 WAL 模式提升并发性能
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()

    def _init_database(self):
        """创建股票实时快照数据表"""
        cursor = self.conn.cursor()

        # 创建股票实时快照数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_rt_snapshot (
                ts_code TEXT NOT NULL PRIMARY KEY,   -- TS 股票代码（如：000001.SZ）
                name TEXT,                           -- 股票名称
                close REAL,                          -- 最新价/收盘价
                open REAL,                           -- 开盘价
                high REAL,                           -- 最高价
                low REAL,                            -- 最低价
                pre_close REAL,                      -- 昨收价
                change REAL,                         -- 涨跌额
                pct_chg REAL,                        -- 涨跌幅（百分比）
                vol REAL,                            -- 成交量（手）
                amount REAL,                         -- 成交额（千元）
                updated_at REAL NOT NULL             -- 数据更新时间戳
            )
        ''')

        # 创建索引以提升查询性能
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_rt_updated_at ON stock_rt_snapshot(updated_at)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_rt_name ON stock_rt_snapshot(name)
        ''')

        self.conn.commit()

    def _get_current_time(self) -> float:
        """获取当前时间戳"""
        return time.time()

    def is_expired(self, ts_code: str) -> bool:
        """
        检查指定股票的缓存是否过期

        参数:
            ts_code: 股票代码

        返回:
            如果缓存不存在或已过期返回 True，否则返回 False
        """
        cursor = self.conn.cursor()
        current_time = self._get_current_time()

        cursor.execute('''
            SELECT updated_at FROM stock_rt_snapshot
            WHERE ts_code = ?
        ''', (ts_code,))

        row = cursor.fetchone()
        if row is None:
            return True  # 无缓存数据，视为过期

        updated_at = row[0]
        return (current_time - updated_at) > self.CACHE_TTL

    def get_snapshot(self, ts_code: str) -> Optional[Dict]:
        """
        获取缓存的股票实时快照

        参数:
            ts_code: 股票代码

        返回:
            包含快照信息的字典，如果缓存不存在或已过期则返回 None
        """
        cursor = self.conn.cursor()
        current_time = self._get_current_time()

        cursor.execute('''
            SELECT
                ts_code, name, close, open, high, low,
                pre_close, change, pct_chg, vol, amount, updated_at
            FROM stock_rt_snapshot
            WHERE ts_code = ?
        ''', (ts_code,))

        row = cursor.fetchone()
        if row is None:
            return None

        # 检查是否过期
        updated_at = row[11]
        if (current_time - updated_at) > self.CACHE_TTL:
            return None  # 已过期

        return {
            'ts_code': row[0],
            'name': row[1],
            'close': row[2],
            'open': row[3],
            'high': row[4],
            'low': row[5],
            'pre_close': row[6],
            'change': row[7],
            'pct_chg': row[8],
            'vol': row[9],
            'amount': row[10],
            'updated_at': row[11]
        }

    def save_snapshot(self, df: pd.DataFrame) -> int:
        """
        保存股票实时快照数据到数据库

        参数:
            df: 包含实时快照数据的 DataFrame，必须包含 ts_code 列

        返回:
            保存的记录数量
        """
        if df.empty:
            return 0

        # 确保必要的列存在
        if 'ts_code' not in df.columns:
            raise ValueError("DataFrame 必须包含 'ts_code' 列")

        cursor = self.conn.cursor()
        current_time = self._get_current_time()
        saved_count = 0

        # 准备插入或更新的数据
        for _, row in df.iterrows():
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO stock_rt_snapshot (
                        ts_code, name, close, open, high, low,
                        pre_close, change, pct_chg, vol, amount, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('ts_code', '')),
                    str(row.get('name', '')) if pd.notna(row.get('name')) else None,
                    row.get('close') if pd.notna(row.get('close')) else None,
                    row.get('open') if pd.notna(row.get('open')) else None,
                    row.get('high') if pd.notna(row.get('high')) else None,
                    row.get('low') if pd.notna(row.get('low')) else None,
                    row.get('pre_close') if pd.notna(row.get('pre_close')) else None,
                    row.get('change') if pd.notna(row.get('change')) else None,
                    row.get('pct_chg') if pd.notna(row.get('pct_chg')) else None,
                    row.get('vol') if pd.notna(row.get('vol')) else None,
                    row.get('amount') if pd.notna(row.get('amount')) else None,
                    current_time
                ))
                saved_count += 1
            except Exception as e:
                import sys
                print(f"保存股票实时快照数据时出错：{str(e)}", file=sys.stderr)
                continue

        self.conn.commit()
        return saved_count

    def get_valid_snapshots(self) -> pd.DataFrame:
        """
        获取所有未过期的缓存数据

        返回:
            DataFrame，包含所有未过期的快照数据
        """
        cursor = self.conn.cursor()
        current_time = self._get_current_time()
        cutoff_time = current_time - self.CACHE_TTL

        cursor.execute('''
            SELECT
                ts_code, name, close, open, high, low,
                pre_close, change, pct_chg, vol, amount, updated_at
            FROM stock_rt_snapshot
            WHERE updated_at >= ?
        ''', (cutoff_time,))

        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()

        columns = [
            'ts_code', 'name', 'close', 'open', 'high', 'low',
            'pre_close', 'change', 'pct_chg', 'vol', 'amount', 'updated_at'
        ]
        df = pd.DataFrame(rows, columns=columns)

        # 转换数据类型
        numeric_columns = ['close', 'open', 'high', 'low', 'pre_close',
                          'change', 'pct_chg', 'vol', 'amount', 'updated_at']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    def clear_snapshot(self, ts_code: Optional[str] = None) -> int:
        """
        清理股票实时快照数据

        参数:
            ts_code: 如果指定，只清理该股票的数据；否则清理所有数据

        返回:
            删除的记录数量
        """
        cursor = self.conn.cursor()

        if ts_code:
            cursor.execute('DELETE FROM stock_rt_snapshot WHERE ts_code = ?', (ts_code,))
        else:
            cursor.execute('DELETE FROM stock_rt_snapshot')

        count = cursor.rowcount
        self.conn.commit()
        return count

    def get_stats(self) -> Dict:
        """获取实时快照缓存统计信息"""
        cursor = self.conn.cursor()
        current_time = self._get_current_time()
        cutoff_time = current_time - self.CACHE_TTL

        # 总记录数
        cursor.execute('SELECT COUNT(*) FROM stock_rt_snapshot')
        total_count = cursor.fetchone()[0]

        # 未过期的记录数
        cursor.execute('''
            SELECT COUNT(*) FROM stock_rt_snapshot
            WHERE updated_at >= ?
        ''', (cutoff_time,))
        valid_count = cursor.fetchone()[0]

        # 已过期的记录数
        expired_count = total_count - valid_count

        # 最新更新时间
        cursor.execute('SELECT MAX(updated_at) FROM stock_rt_snapshot')
        latest_update = cursor.fetchone()[0]

        return {
            'total_records': total_count,
            'valid_records': valid_count,
            'expired_records': expired_count,
            'latest_update': latest_update
        }

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


# 创建全局股票实时行情缓存管理器实例
stock_rt_cache_manager = StockRtCacheManager()

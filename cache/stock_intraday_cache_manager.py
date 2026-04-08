"""股票日内分钟级快照数据专用缓存管理器"""
import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict
import pandas as pd
from config.settings import CACHE_DB
from datetime import datetime


class StockIntradayCacheManager:
    """股票日内分钟级快照数据专用缓存管理器

    用途：
    1. 存储股票日内分钟级快照数据（用于同刻量比计算）
    2. 支持按股票代码和时间查询历史快照
    3. 支持批量保存实时快照数据

    表结构：
    - stock_intraday_snapshot: 股票日内快照表
      - ts_code: 股票代码
      - trade_time: 交易时间 (HH:MM:SS，主键之一)
      - trade_date: 交易日期 (YYYYMMDD)
      - close: 价格
      - vol: 累计成交量
      - amount: 累计成交额
      - created_at: 创建时间戳
    """

    def __init__(self, db_path: Path = CACHE_DB):
        """初始化股票日内快照缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # 启用 WAL 模式提升并发性能
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()

    def _init_database(self):
        """创建股票日内快照数据表"""
        cursor = self.conn.cursor()

        # 创建股票日内快照数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_intraday_snapshot (
                ts_code TEXT NOT NULL,           -- TS 股票代码（如：000001.SZ）
                trade_time TEXT NOT NULL,        -- 交易时间 (HH:MM:SS 格式)
                trade_date TEXT NOT NULL,        -- 交易日期 (YYYYMMDD 格式)
                close REAL,                      -- 价格/最新价
                vol REAL,                        -- 累计成交量（手）
                amount REAL,                     -- 累计成交额（千元）
                pre_close REAL,                  -- 昨收价
                open REAL,                       -- 开盘价
                high REAL,                       -- 最高价
                low REAL,                        -- 最低价
                created_at REAL NOT NULL,        -- 数据创建时间戳
                PRIMARY KEY (ts_code, trade_time)
            )
        ''')

        # 创建索引以提升查询性能
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_intraday_ts_code ON stock_intraday_snapshot(ts_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_intraday_trade_date ON stock_intraday_snapshot(trade_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_intraday_trade_time ON stock_intraday_snapshot(trade_time)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_intraday_ts_code_date ON stock_intraday_snapshot(ts_code, trade_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_intraday_created_at ON stock_intraday_snapshot(created_at)
        ''')

        self.conn.commit()

    def save_snapshot(self, df: pd.DataFrame) -> int:
        """
        保存股票日内快照数据到数据库

        参数:
            df: 包含日内快照数据的 DataFrame，必须包含 ts_code 列

        返回:
            保存的记录数量
        """
        if df.empty:
            return 0

        # 确保必要的列存在
        required_columns = ['ts_code']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"DataFrame 必须包含 '{col}' 列")

        cursor = self.conn.cursor()
        current_time = time.time()
        saved_count = 0

        # 准备插入或更新的数据
        for _, row in df.iterrows():
            try:
                # 获取交易时间，如果没有则使用当前时间
                trade_time = row.get('trade_time', '')
                if not trade_time:
                    # 尝试从其他字段推断
                    if 'time' in row:
                        trade_time = str(row['time'])
                    else:
                        continue  # 没有时间字段，跳过

                # 获取交易日期，如果没有则使用今天
                trade_date = row.get('trade_date', '')
                if not trade_date:
                    if 'date' in row:
                        trade_date = str(row['date'])
                    else:
                        trade_date = datetime.now().strftime('%Y%m%d')

                cursor.execute('''
                    INSERT OR REPLACE INTO stock_intraday_snapshot (
                        ts_code, trade_time, trade_date, close, vol, amount,
                        pre_close, open, high, low, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('ts_code', '')),
                    str(trade_time),
                    str(trade_date),
                    row.get('close') if pd.notna(row.get('close')) else None,
                    row.get('vol') if pd.notna(row.get('vol')) else None,
                    row.get('amount') if pd.notna(row.get('amount')) else None,
                    row.get('pre_close') if pd.notna(row.get('pre_close')) else None,
                    row.get('open') if pd.notna(row.get('open')) else None,
                    row.get('high') if pd.notna(row.get('high')) else None,
                    row.get('low') if pd.notna(row.get('low')) else None,
                    current_time
                ))
                saved_count += 1
            except Exception as e:
                # 记录错误但继续处理其他记录
                import sys
                print(f"保存股票日内快照数据时出错：{str(e)}", file=sys.stderr)
                continue

        self.conn.commit()
        return saved_count

    def get_historical_snapshot(
        self,
        ts_code: str,
        trade_date: str,
        trade_time: str
    ) -> Optional[Dict]:
        """
        获取历史某一时刻的快照数据（最接近指定时间的记录）

        参数:
            ts_code: 股票代码（如：600519.SH）
            trade_date: 历史日期 (YYYYMMDD)
            trade_time: 历史时间 (HH:MM:SS)

        返回:
            包含快照信息的字典，如果未找到则返回 None
            字典格式：
            {
                'ts_code': str,
                'trade_date': str,
                'trade_time': str,
                'close': float,
                'vol': float,
                'amount': float,
                'created_at': float
            }
        """
        cursor = self.conn.cursor()

        # 查询指定日期，时间最接近的记录
        # 优先查找等于或早于指定时间的记录，按时间降序取第一条
        cursor.execute('''
            SELECT
                ts_code, trade_date, trade_time, close, vol, amount,
                pre_close, open, high, low, created_at
            FROM stock_intraday_snapshot
            WHERE ts_code = ? AND trade_date = ? AND trade_time <= ?
            ORDER BY trade_time DESC
            LIMIT 1
        ''', (ts_code, trade_date, trade_time))

        row = cursor.fetchone()
        if not row:
            # 如果当天没有数据，尝试查找最近一天的数据
            cursor.execute('''
                SELECT
                    ts_code, trade_date, trade_time, close, vol, amount,
                    pre_close, open, high, low, created_at
                FROM stock_intraday_snapshot
                WHERE ts_code = ? AND trade_date < ?
                ORDER BY trade_date DESC, trade_time DESC
                LIMIT 1
            ''', (ts_code, trade_date))
            row = cursor.fetchone()

        if not row:
            return None

        return {
            'ts_code': row[0],
            'trade_date': row[1],
            'trade_time': row[2],
            'close': row[3],
            'vol': row[4],
            'amount': row[5],
            'pre_close': row[6],
            'open': row[7],
            'high': row[8],
            'low': row[9],
            'created_at': row[10]
        }

    def get_snapshot_by_time(
        self,
        trade_date: str,
        trade_time: str,
        ts_codes: Optional[list] = None
    ) -> pd.DataFrame:
        """
        获取指定时刻所有股票（或指定股票）的快照数据

        参数:
            trade_date: 交易日期 (YYYYMMDD)
            trade_time: 交易时间 (HH:MM:SS)
            ts_codes: 股票代码列表，None 表示获取所有股票

        返回:
            DataFrame，包含匹配的股票快照信息
        """
        cursor = self.conn.cursor()

        # 构建查询条件
        if ts_codes:
            placeholders = ','.join(['?'] * len(ts_codes))
            query = f'''
                SELECT
                    ts_code, trade_date, trade_time, close, vol, amount,
                    pre_close, open, high, low, created_at
                FROM stock_intraday_snapshot
                WHERE trade_date = ? AND trade_time = ? AND ts_code IN ({placeholders})
            '''
            params = [trade_date, trade_time] + list(ts_codes)
        else:
            query = '''
                SELECT
                    ts_code, trade_date, trade_time, close, vol, amount,
                    pre_close, open, high, low, created_at
                FROM stock_intraday_snapshot
                WHERE trade_date = ? AND trade_time = ?
            '''
            params = [trade_date, trade_time]

        cursor.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            return pd.DataFrame()

        # 转换为 DataFrame
        columns = [
            'ts_code', 'trade_date', 'trade_time', 'close', 'vol', 'amount',
            'pre_close', 'open', 'high', 'low', 'created_at'
        ]
        df = pd.DataFrame(rows, columns=columns)

        # 转换数据类型
        numeric_columns = ['close', 'vol', 'amount', 'pre_close', 'open', 'high', 'low', 'created_at']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    def get_latest_snapshot(self, ts_code: str) -> Optional[Dict]:
        """
        获取单只股票的最新快照数据

        参数:
            ts_code: 股票代码

        返回:
            包含最新快照信息的字典，如果未找到则返回 None
        """
        cursor = self.conn.cursor()

        cursor.execute('''
            SELECT
                ts_code, trade_date, trade_time, close, vol, amount,
                pre_close, open, high, low, created_at
            FROM stock_intraday_snapshot
            WHERE ts_code = ?
            ORDER BY trade_date DESC, trade_time DESC
            LIMIT 1
        ''', (ts_code,))

        row = cursor.fetchone()
        if not row:
            return None

        return {
            'ts_code': row[0],
            'trade_date': row[1],
            'trade_time': row[2],
            'close': row[3],
            'vol': row[4],
            'amount': row[5],
            'pre_close': row[6],
            'open': row[7],
            'high': row[8],
            'low': row[9],
            'created_at': row[10]
        }

    def has_data(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None
    ) -> bool:
        """
        检查是否存在指定的数据

        参数:
            ts_code: 股票代码（可选）
            trade_date: 交易日期（可选）

        返回:
            如果存在数据返回 True，否则返回 False
        """
        cursor = self.conn.cursor()
        conditions = []
        params = []

        if ts_code:
            conditions.append('ts_code = ?')
            params.append(ts_code)
        if trade_date:
            conditions.append('trade_date = ?')
            params.append(trade_date)

        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        cursor.execute(f'SELECT COUNT(*) FROM stock_intraday_snapshot WHERE {where_clause}', params)
        count = cursor.fetchone()[0]
        return count > 0

    def get_stats(self) -> Dict:
        """获取股票日内快照数据统计信息"""
        cursor = self.conn.cursor()

        # 总统计
        cursor.execute('SELECT COUNT(*) FROM stock_intraday_snapshot')
        total_count = cursor.fetchone()[0]

        # 按日期统计
        cursor.execute('''
            SELECT trade_date, COUNT(*) as count
            FROM stock_intraday_snapshot
            GROUP BY trade_date
            ORDER BY trade_date DESC
        ''')
        by_date = {row[0]: row[1] for row in cursor.fetchall()}

        # 按股票统计
        cursor.execute('''
            SELECT ts_code, COUNT(*) as count
            FROM stock_intraday_snapshot
            GROUP BY ts_code
            ORDER BY count DESC
        ''')
        by_stock = {row[0]: row[1] for row in cursor.fetchall()}

        return {
            'total_records': total_count,
            'by_date': by_date,
            'by_stock': by_stock
        }

    def clear_snapshot(self, ts_code: Optional[str] = None) -> int:
        """
        清理股票日内快照数据

        参数:
            ts_code: 如果指定，只清理该股票的数据；否则清理所有数据

        返回:
            删除的记录数量
        """
        cursor = self.conn.cursor()

        if ts_code:
            cursor.execute('DELETE FROM stock_intraday_snapshot WHERE ts_code = ?', (ts_code,))
        else:
            cursor.execute('DELETE FROM stock_intraday_snapshot')

        count = cursor.rowcount
        self.conn.commit()
        return count

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


# 创建全局股票日内快照缓存管理器实例
stock_intraday_cache_manager = StockIntradayCacheManager()

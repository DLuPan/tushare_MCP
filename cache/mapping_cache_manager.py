"""股票与板块映射关系专用缓存管理器"""
import sqlite3
import json
import time
from pathlib import Path
from typing import Optional, Dict, List
import pandas as pd
from config.settings import CACHE_DB


class MappingCacheManager:
    """股票与板块映射关系专用缓存管理器

    用途：
    1. 存储股票与申万二级行业、东财行业、东财概念的映射关系
    2. 支持按股票代码查询映射
    3. 支持按板块类型和代码搜索股票

    表结构：
    - stock_sector_mapping: 股票板块映射表
      - ts_code: 股票代码（主键）
      - name: 股票名称
      - sw_l2_code/sw_l2_name: 申万二级行业代码/名称
      - em_industry_code/em_industry_name: 东财行业代码/名称
      - em_concept_codes: 东财概念代码列表（JSON 格式）
      - em_concept_names: 东财概念名称列表（JSON 格式）
      - updated_at: 更新时间戳
    """

    def __init__(self, db_path: Path = CACHE_DB):
        """初始化映射缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # 启用 WAL 模式提升并发性能
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()

    def _init_database(self):
        """创建股票板块映射表"""
        cursor = self.conn.cursor()

        # 创建股票板块映射表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_sector_mapping (
                ts_code TEXT NOT NULL PRIMARY KEY,     -- 股票代码（如：000001.SZ）
                name TEXT,                             -- 股票名称
                sw_l2_code TEXT,                       -- 申万二级行业代码
                sw_l2_name TEXT,                       -- 申万二级行业名称
                em_industry_code TEXT,                 -- 东财行业代码
                em_industry_name TEXT,                 -- 东财行业名称
                em_concept_codes TEXT,                 -- 东财概念代码列表（JSON 格式）
                em_concept_names TEXT,                 -- 东财概念名称列表（JSON 格式）
                updated_at REAL NOT NULL               -- 数据更新时间戳
            )
        ''')

        # 创建索引以提升查询性能
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_mapping_name ON stock_sector_mapping(name)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_mapping_sw_l2_code ON stock_sector_mapping(sw_l2_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_mapping_sw_l2_name ON stock_sector_mapping(sw_l2_name)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_mapping_em_industry_code ON stock_sector_mapping(em_industry_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_mapping_em_industry_name ON stock_sector_mapping(em_industry_name)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_mapping_updated_at ON stock_sector_mapping(updated_at)
        ''')

        self.conn.commit()

    def save_mapping(self, df: pd.DataFrame) -> int:
        """
        保存股票板块映射数据到数据库

        参数:
            df: 包含映射数据的 DataFrame，必须包含 ts_code 列

        返回:
            保存的记录数量
        """
        if df.empty:
            return 0

        # 确保必要的列存在
        if 'ts_code' not in df.columns:
            raise ValueError("DataFrame 必须包含 'ts_code' 列")

        cursor = self.conn.cursor()
        current_time = time.time()
        saved_count = 0

        # 准备插入或更新的数据
        for _, row in df.iterrows():
            try:
                # 处理列表类型的字段（转为 JSON 字符串存储）
                em_concept_codes = row.get('em_concept_codes', [])
                em_concept_names = row.get('em_concept_names', [])

                # 确保是列表类型
                if isinstance(em_concept_codes, str):
                    em_concept_codes = [em_concept_codes] if em_concept_codes else []
                if isinstance(em_concept_names, str):
                    em_concept_names = [em_concept_names] if em_concept_names else []

                # 转为 JSON 字符串
                codes_json = json.dumps(em_concept_codes, ensure_ascii=False)
                names_json = json.dumps(em_concept_names, ensure_ascii=False)

                cursor.execute('''
                    INSERT OR REPLACE INTO stock_sector_mapping (
                        ts_code, name, sw_l2_code, sw_l2_name,
                        em_industry_code, em_industry_name,
                        em_concept_codes, em_concept_names, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('ts_code', '')),
                    str(row.get('name', '')),
                    str(row.get('sw_l2_code', '')) if pd.notna(row.get('sw_l2_code')) else None,
                    str(row.get('sw_l2_name', '')) if pd.notna(row.get('sw_l2_name')) else None,
                    str(row.get('em_industry_code', '')) if pd.notna(row.get('em_industry_code')) else None,
                    str(row.get('em_industry_name', '')) if pd.notna(row.get('em_industry_name')) else None,
                    codes_json,
                    names_json,
                    current_time
                ))
                saved_count += 1
            except Exception as e:
                # 记录错误但继续处理其他记录
                import sys
                print(f"保存股票板块映射数据时出错：{str(e)}", file=sys.stderr)
                continue

        self.conn.commit()
        return saved_count

    def get_mapping_by_code(self, ts_code: str) -> Optional[Dict]:
        """
        根据股票代码获取映射关系

        参数:
            ts_code: 股票代码（如：600519.SH）

        返回:
            包含映射信息的字典，如果未找到则返回 None
            字典格式：
            {
                'ts_code': str,
                'name': str,
                'sw_l2_code': str,
                'sw_l2_name': str,
                'em_industry_code': str,
                'em_industry_name': str,
                'em_concept_codes': List[str],
                'em_concept_names': List[str],
                'updated_at': float
            }
        """
        cursor = self.conn.cursor()

        cursor.execute('''
            SELECT
                ts_code, name, sw_l2_code, sw_l2_name,
                em_industry_code, em_industry_name,
                em_concept_codes, em_concept_names, updated_at
            FROM stock_sector_mapping
            WHERE ts_code = ?
        ''', (ts_code,))

        row = cursor.fetchone()
        if not row:
            return None

        # 解析 JSON 字段
        try:
            em_concept_codes = json.loads(row[6]) if row[6] else []
            em_concept_names = json.loads(row[7]) if row[7] else []
        except (json.JSONDecodeError, TypeError):
            em_concept_codes = []
            em_concept_names = []

        return {
            'ts_code': row[0],
            'name': row[1] or '',
            'sw_l2_code': row[2] or '',
            'sw_l2_name': row[3] or '',
            'em_industry_code': row[4] or '',
            'em_industry_name': row[5] or '',
            'em_concept_codes': em_concept_codes,
            'em_concept_names': em_concept_names,
            'updated_at': row[8]
        }

    def search_by_sector(self, sector_type: str, sector_code: str) -> pd.DataFrame:
        """
        根据板块类型和代码搜索股票

        参数:
            sector_type: 板块类型 ('sw_l2', 'em_industry', 'em_concept')
            sector_code: 板块代码（如：BK1184.DC, 801053.SI）

        返回:
            DataFrame，包含匹配的股票信息
        """
        cursor = self.conn.cursor()

        # 根据板块类型构建查询条件
        if sector_type == 'sw_l2':
            # 申万二级行业：需要匹配代码或名称
            query = '''
                SELECT ts_code, name, sw_l2_code, sw_l2_name,
                       em_industry_code, em_industry_name,
                       em_concept_codes, em_concept_names, updated_at
                FROM stock_sector_mapping
                WHERE sw_l2_code = ? OR sw_l2_name = ?
            '''
            params = [sector_code, sector_code]
        elif sector_type == 'em_industry':
            # 东财行业：需要匹配代码或名称
            query = '''
                SELECT ts_code, name, sw_l2_code, sw_l2_name,
                       em_industry_code, em_industry_name,
                       em_concept_codes, em_concept_names, updated_at
                FROM stock_sector_mapping
                WHERE em_industry_code = ? OR em_industry_name = ?
            '''
            params = [sector_code, sector_code]
        elif sector_type == 'em_concept':
            # 东财概念：需要在 JSON 数组中查找
            query = '''
                SELECT ts_code, name, sw_l2_code, sw_l2_name,
                       em_industry_code, em_industry_name,
                       em_concept_codes, em_concept_names, updated_at
                FROM stock_sector_mapping
                WHERE em_concept_codes LIKE ? OR em_concept_names LIKE ?
            '''
            # 使用 LIKE 进行 JSON 数组匹配
            pattern = f'%{sector_code}%'
            params = [pattern, pattern]
        else:
            return pd.DataFrame()

        cursor.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            return pd.DataFrame()

        # 转换为 DataFrame
        columns = [
            'ts_code', 'name', 'sw_l2_code', 'sw_l2_name',
            'em_industry_code', 'em_industry_name',
            'em_concept_codes', 'em_concept_names', 'updated_at'
        ]
        df = pd.DataFrame(rows, columns=columns)

        # 解析 JSON 字段
        def parse_json(x):
            if isinstance(x, str):
                try:
                    return json.loads(x)
                except:
                    return []
            return x if isinstance(x, list) else []

        df['em_concept_codes'] = df['em_concept_codes'].apply(parse_json)
        df['em_concept_names'] = df['em_concept_names'].apply(parse_json)

        # 转换时间戳
        df['updated_at'] = pd.to_numeric(df['updated_at'], errors='coerce')

        return df

    def get_stats(self) -> Dict:
        """获取映射数据统计信息"""
        cursor = self.conn.cursor()

        # 总统计
        cursor.execute('SELECT COUNT(*) FROM stock_sector_mapping')
        total_count = cursor.fetchone()[0]

        # 按申万二级行业统计
        cursor.execute('''
            SELECT sw_l2_name, COUNT(*) as count
            FROM stock_sector_mapping
            WHERE sw_l2_name IS NOT NULL AND sw_l2_name != ''
            GROUP BY sw_l2_name
            ORDER BY count DESC
        ''')
        sw_l2_stats = {row[0]: row[1] for row in cursor.fetchall()}

        # 按东财行业统计
        cursor.execute('''
            SELECT em_industry_name, COUNT(*) as count
            FROM stock_sector_mapping
            WHERE em_industry_name IS NOT NULL AND em_industry_name != ''
            GROUP BY em_industry_name
            ORDER BY count DESC
        ''')
        em_industry_stats = {row[0]: row[1] for row in cursor.fetchall()}

        return {
            'total_stocks': total_count,
            'sw_l2_sectors': sw_l2_stats,
            'em_industries': em_industry_stats
        }

    def clear_mapping(self) -> int:
        """
        清理所有映射数据

        返回:
            删除的记录数量
        """
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM stock_sector_mapping')
        count = cursor.rowcount
        self.conn.commit()
        return count

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


# 创建全局映射缓存管理器实例
mapping_cache_manager = MappingCacheManager()

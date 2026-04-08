"""创建T-1（前一天）预计算数据表

整合所有T-1数据到一张表中，包含：
- 股票基本信息（流通股本、总市值）
- 机构数据（5日机构净买入、上榜次数）
- 筹码数据（胜率、筹码集中度）
- 融资融券数据（融资余额占比）
- 日线统计数据（前收盘价、前成交量、前平均成交额）

每个股票只有一条记录（T-1数据）。
"""
import sqlite3
import time
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import CACHE_DB


def create_t1_tables(db_path: Path = CACHE_DB):
    """创建T-1预计算数据表（整合为一张表）"""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    # 启用WAL模式提升并发性能
    conn.execute('PRAGMA journal_mode=WAL')
    cursor = conn.cursor()
    
    try:
        # 检查现有 t1_data 表结构
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='t1_data'")
        if cursor.fetchone():
            cursor.execute("PRAGMA table_info(t1_data)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'trade_date' not in columns:
                print("[WARNING] 检测到旧版 t1_data 表缺少 trade_date 字段，正在升级结构...")
                cursor.execute("DROP TABLE t1_data")

        # 创建全历史 T-1 数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS t1_data (
                ts_code TEXT NOT NULL,              -- 股票代码
                trade_date TEXT NOT NULL,           -- 数据归属日期 (T-1日)
                
                -- 股票基本信息
                float_share REAL,                    -- 流通股本（万股）
                total_mv REAL,                       -- 总市值（万元）
                
                -- 机构数据
                sum_inst_net REAL,                  -- 5日机构净买入（万元）
                list_count INTEGER,                 -- 上榜次数
                
                -- 筹码数据
                winner_rate REAL,                   -- 胜率（0-100）
                cost_concentration REAL,            -- 筹码集中度
                
                -- 融资融券数据
                margin_cap_ratio REAL,              -- 融资余额占比（0-1）
                
                -- 日线统计数据
                pre_close REAL,                     -- 前收盘价
                pre_vol INTEGER,                    -- 前成交量（手）
                pre_ats REAL,                       -- 前平均成交额（万元）
                
                -- 元数据
                updated_at REAL NOT NULL,           -- 数据采集时间戳
                PRIMARY KEY (ts_code, trade_date)   -- 联合主键支持历史存储
            )
        ''')
        
        # 增加日期索引以优化查询
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_t1_data_trade_date 
            ON t1_data(trade_date)
        ''')
        
        conn.commit()
        print("[SUCCESS] T-1历史数据表创建/升级成功！")
        print("\n表名: t1_data (支持全历史存储)")
        
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] 创建表时出错: {str(e)}")
        raise
    finally:
        conn.close()


def verify_tables(db_path: Path = CACHE_DB):
    """验证表是否创建成功"""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    cursor = conn.cursor()
    
    table_name = 't1_data'
    
    cursor.execute(f'''
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='{table_name}'
    ''')
    
    exists = cursor.fetchone() is not None
    
    print("\n📊 表验证结果：")
    if exists:
        # 获取表结构
        cursor.execute(f'PRAGMA table_info({table_name})')
        columns = cursor.fetchall()
        print(f"  ✅ {table_name}: {len(columns)} 列")
        print("\n  字段详情：")
        for col in columns:
            col_name, col_type = col[1], col[2]
            nullable = "" if col[3] == 0 else " (可空)"
            pk = " [主键]" if col[5] == 1 else ""
            default = f" DEFAULT {col[4]}" if col[4] else ""
            print(f"     - {col_name}: {col_type}{nullable}{pk}{default}")
    else:
        print(f"  ❌ {table_name}: 未找到")
    
    conn.close()


if __name__ == '__main__':
    import sys
    print("开始创建T-1预计算数据表...", flush=True)
    print(f"数据库路径: {CACHE_DB}", flush=True)
    print("-" * 60, flush=True)
    
    try:
        create_t1_tables()
        verify_tables()
        print("\n" + "-" * 60, flush=True)
        print("✅ 完成！", flush=True)
    except Exception as e:
        print(f"❌ 错误: {str(e)}", file=sys.stderr, flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


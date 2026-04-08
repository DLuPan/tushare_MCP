"""机构抱团扫描 MCP 工具 (Institutional Track)

该模块提供机构抱团分析工具，综合分析机构偏好赛道、调研热度和龙虎榜资金。

分析维度:
- 模块1：黄金赛道锁定 - 申万二级行业 Alpha 排名 + 机构偏好筛选
- 模块2：机构雷达扫描 - 调研热度 + 龙虎榜抢筹信号
- 模块3：机构票池输出 - 汇总评级并输出
"""
import tushare as ts
import pandas as pd
import re
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional, Dict, Any, List, Tuple
from config.token_manager import get_tushare_token
from cache.cache_manager import cache_manager

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP


# 机构偏好行业关键词（用于识别机构重点关注的行业）
INSTITUTIONAL_PREFERRED_KEYWORDS = [
    "半导体", "集成电路", "芯片",
    "电池", "储能",
    "白酒",
    "医疗服务", "创新药", "医药",
    "银行", "保险", "证券",
    "新能源", "光伏", "风电",
    "消费电子", "汽车零部件",
]

# 排除的行业关键词
EXCLUDE_INDUSTRY_KEYWORDS = ["综合", "其他"]


def register_inst_track_tools(mcp: "FastMCP"):
    """注册机构抱团扫描工具"""
    
    @mcp.tool()
    def inst_track_scan(
        trade_date: str = "",
        top_sectors: int = 15,
        survey_threshold: int = 20,
        survey_days: int = 30,
        top_inst_days: int = 5
    ) -> str:
        """
        机构抱团扫描 (Institutional Track) - 机构资金动向综合分析
        
        参数:
            trade_date: 分析日期（YYYYMMDD格式，默认使用最新交易日）
            top_sectors: 二级行业Alpha排名取前N个（默认15）
            survey_threshold: 调研热度阈值，近N日内调研家数（默认20）
            survey_days: 调研统计周期天数（默认30）
            top_inst_days: 龙虎榜回溯天数（默认5）
        
        返回:
            包含三个分析模块的机构抱团扫描报告
        
        分析模块:
            1. 黄金赛道锁定 - 筛选机构偏好的强势二级行业
            2. 机构雷达扫描 - 调研热度 + 龙虎榜抢筹信号
            3. 机构票池输出 - 标签评级汇总
        
        标签评级:
            - 🏷️ [机构关注·趋势稳健]: 调研热度高
            - 🌟 [机构抢筹·核心龙头]: 调研热度高 + 龙虎榜机构净买入
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 确定分析日期
            if not trade_date:
                trade_date = _get_latest_trade_date()
            
            # 模块1：锁定黄金赛道
            tracks_result = _select_golden_tracks(trade_date, top_sectors)
            
            # 模块2：机构雷达扫描
            radar_result = _scan_institutional_radar(
                trade_date, 
                tracks_result,
                survey_threshold,
                survey_days,
                top_inst_days
            )
            
            # 模块3：生成机构票池
            pool_result = _generate_stock_pool(radar_result)
            
            # 生成综合报告
            report = _format_inst_track_report(
                trade_date,
                tracks_result,
                radar_result,
                pool_result,
                survey_threshold,
                survey_days,
                top_inst_days
            )
            
            return report
            # return "Test Success: Report generated but suppressed for testing."
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"机构抱团扫描失败：{str(e)}\n详细信息：{error_detail}"


def _get_latest_trade_date() -> str:
    """获取最新交易日"""
    today = datetime.now()
    # 如果是周末，回退到周五
    while today.weekday() >= 5:
        today -= timedelta(days=1)
    return today.strftime("%Y%m%d")


def _get_date_range(end_date: str, days: int) -> Tuple[str, str]:
    """计算日期范围"""
    end = datetime.strptime(end_date, "%Y%m%d")
    start = end - timedelta(days=days)
    return start.strftime("%Y%m%d"), end_date


def _select_golden_tracks(trade_date: str, top_sectors: int) -> Dict[str, Any]:
    """
    模块1: 锁定黄金赛道
    
    获取 Top N 申万二级行业，筛选机构偏好行业
    """
    result = {
        "success": False,
        "top_sectors": [],
        "golden_tracks": [],
        "error": None
    }
    
    try:
        from tools.alpha_strategy_analyzer import rank_sectors_alpha
        
        # 申万二级行业代码列表
        sector_codes = [
            "801012.SI", "801014.SI", "801015.SI", "801016.SI", "801017.SI", "801018.SI",
            "801032.SI", "801033.SI", "801034.SI", "801036.SI", "801037.SI", "801038.SI", "801039.SI",
            "801043.SI", "801044.SI", "801045.SI",
            "801051.SI", "801053.SI", "801054.SI", "801055.SI", "801056.SI",
            "801072.SI", "801074.SI", "801076.SI", "801077.SI", "801078.SI",
            "801081.SI", "801082.SI", "801083.SI", "801084.SI", "801085.SI", "801086.SI",
            "801092.SI", "801093.SI", "801095.SI", "801096.SI",
            "801101.SI", "801102.SI", "801103.SI", "801104.SI",
            "801111.SI", "801112.SI", "801113.SI", "801114.SI", "801115.SI", "801116.SI",
            "801124.SI", "801125.SI", "801126.SI", "801127.SI", "801128.SI", "801129.SI",
            "801131.SI", "801132.SI", "801133.SI",
            "801141.SI", "801142.SI", "801143.SI", "801145.SI",
            "801151.SI", "801152.SI", "801153.SI", "801154.SI", "801155.SI", "801156.SI",
            "801161.SI", "801163.SI",
            "801178.SI", "801179.SI",
            "801181.SI", "801183.SI",
            "801191.SI", "801193.SI", "801194.SI",
            "801202.SI", "801203.SI", "801204.SI", "801206.SI",
            "801218.SI", "801219.SI",
            "801223.SI",
            "801231.SI",
            "801711.SI", "801712.SI", "801713.SI",
            "801721.SI", "801722.SI", "801723.SI", "801724.SI", "801726.SI",
            "801731.SI", "801733.SI", "801735.SI", "801736.SI", "801737.SI", "801738.SI",
            "801741.SI", "801742.SI", "801743.SI", "801744.SI", "801745.SI",
            "801764.SI", "801765.SI", "801766.SI", "801767.SI", "801769.SI",
            "801782.SI", "801783.SI", "801784.SI", "801785.SI",
            "801881.SI",
            "801951.SI", "801952.SI",
            "801962.SI", "801963.SI",
            "801971.SI", "801972.SI",
            "801981.SI", "801982.SI",
            "801991.SI", "801992.SI", "801993.SI", "801994.SI", "801995.SI"
        ]
        
        # 获取行业名称映射
        pro = ts.pro_api()
        try:
            classify_df = pro.index_classify(level='L2', src='SW2021')
            name_map = dict(zip(classify_df['index_code'], classify_df['industry_name']))
        except Exception:
            name_map = {}
        
        # 进行 Alpha 排名
        df = rank_sectors_alpha(sector_codes, "000300.SH", trade_date)
        
        if df.empty:
            result["error"] = "无法获取行业 Alpha 排名数据"
            return result
        
        # 取前 top_sectors 名
        df = df.head(top_sectors)
        
        # 添加行业名称
        df['name'] = df['sector_code'].map(name_map).fillna(df['sector_code'])
        
        top_sectors_list = []
        golden_tracks = []
        
        for _, row in df.iterrows():
            sector_code = row['sector_code']
            sector_name = row.get('name', sector_code)
            alpha = row['score'] * 100 if pd.notna(row.get('score')) else 0
            
            # 检查是否是机构偏好行业
            is_preferred = any(kw in sector_name for kw in INSTITUTIONAL_PREFERRED_KEYWORDS)
            
            # 检查是否需要排除
            is_excluded = any(kw in sector_name for kw in EXCLUDE_INDUSTRY_KEYWORDS)
            
            sector_info = {
                "code": sector_code,
                "name": sector_name,
                "alpha": alpha,
                "is_preferred": is_preferred and not is_excluded
            }
            
            top_sectors_list.append(sector_info)
            
            if is_preferred and not is_excluded:
                golden_tracks.append(sector_info)
        
        # 取前3个黄金赛道
        result["success"] = True
        result["top_sectors"] = top_sectors_list
        result["golden_tracks"] = golden_tracks[:3] if golden_tracks else top_sectors_list[:3]
        
    except Exception as e:
        result["error"] = str(e)
    
    return result


def _scan_institutional_radar(
    trade_date: str,
    tracks_result: Dict[str, Any],
    survey_threshold: int,
    survey_days: int,
    top_inst_days: int
) -> Dict[str, Any]:
    """
    模块2: 机构雷达扫描
    
    扫描调研热度和龙虎榜抢筹信号
    """
    result = {
        "success": False,
        "survey_stocks": [],  # 调研热度高的股票
        "top_inst_stocks": [],  # 龙虎榜机构买入的股票
        "all_members": [],  # 所有成分股
        "error": None
    }
    
    if not tracks_result.get("success") or not tracks_result.get("golden_tracks"):
        result["error"] = "无法获取黄金赛道数据"
        return result
    
    try:
        pro = ts.pro_api()
        
        # 获取黄金赛道的成分股
        all_members = []
        for track in tracks_result["golden_tracks"]:
            track_code = track["code"]
            track_name = track["name"]
            
            try:
                # 使用 index_member_all 获取成分股
                members_df = pro.index_member_all(l2_code=track_code)
                
                if members_df is not None and not members_df.empty:
                    for _, row in members_df.iterrows():
                        all_members.append({
                            "ts_code": row["ts_code"],
                            "name": row.get("name", ""),
                            "track_code": track_code,
                            "track_name": track_name
                        })
            except Exception:
                pass
        
        result["all_members"] = all_members
        
        if not all_members:
            result["success"] = True
            result["note"] = "无法获取成分股数据"
            return result
        
        # A. 扫描调研热度
        start_date, end_date = _get_date_range(trade_date, survey_days)
        survey_stocks = []
        
        # 按股票分组统计调研次数
        stock_codes = list(set([m["ts_code"] for m in all_members]))
        
        # 批量查询调研数据（按日期范围）
        try:
            survey_df = pro.stk_surv(start_date=start_date, end_date=end_date)
            
            if survey_df is not None and not survey_df.empty:
                # 统计每只股票的调研次数
                survey_counts = survey_df.groupby('ts_code').size().to_dict()
                
                for member in all_members:
                    ts_code = member["ts_code"]
                    count = survey_counts.get(ts_code, 0)
                    
                    if count >= survey_threshold:
                        survey_stocks.append({
                            "ts_code": ts_code,
                            "name": member["name"],
                            "track_name": member["track_name"],
                            "survey_count": count
                        })
        except Exception:
            pass
        
        result["survey_stocks"] = sorted(survey_stocks, key=lambda x: x.get("survey_count", 0), reverse=True)
        
        # B. 扫描龙虎榜机构买入
        top_inst_stocks = []
        
        # 获取近N日的龙虎榜数据
        for i in range(top_inst_days):
            check_date = datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=i)
            # 跳过周末
            if check_date.weekday() >= 5:
                continue
            check_date_str = check_date.strftime("%Y%m%d")
            
            try:
                inst_df = pro.top_inst(trade_date=check_date_str)
                
                if inst_df is not None and not inst_df.empty:
                    # 筛选机构专用席位
                    inst_df = inst_df[inst_df['exalter'].str.contains('机构专用', na=False)]
                    
                    # 计算机构净买入
                    for ts_code in inst_df['ts_code'].unique():
                        if ts_code not in stock_codes:
                            continue
                        
                        code_df = inst_df[inst_df['ts_code'] == ts_code]
                        buy_amount = code_df[code_df['side'] == 0]['buy'].sum()
                        sell_amount = code_df[code_df['side'] == 1]['sell'].sum()
                        net_buy = buy_amount - sell_amount
                        
                        if net_buy > 0:
                            # 查找股票信息
                            member_info = next((m for m in all_members if m["ts_code"] == ts_code), None)
                            if member_info:
                                top_inst_stocks.append({
                                    "ts_code": ts_code,
                                    "name": member_info["name"],
                                    "track_name": member_info["track_name"],
                                    "net_buy": net_buy / 10000,  # 转换为万元
                                    "date": check_date_str
                                })
            except Exception:
                pass
        
        # 去重，保留净买入最大的记录
        unique_inst_stocks = {}
        for stock in top_inst_stocks:
            ts_code = stock["ts_code"]
            if ts_code not in unique_inst_stocks or stock["net_buy"] > unique_inst_stocks[ts_code]["net_buy"]:
                unique_inst_stocks[ts_code] = stock
        
        result["top_inst_stocks"] = sorted(unique_inst_stocks.values(), key=lambda x: x.get("net_buy", 0), reverse=True)
        result["success"] = True
        
    except Exception as e:
        result["error"] = str(e)
        result["success"] = True
    
    return result


def _generate_stock_pool(radar_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    模块3: 生成机构票池
    
    根据扫描结果生成带标签的股票池
    """
    result = {
        "success": False,
        "core_stocks": [],  # 核心龙头 (A + B)
        "attention_stocks": [],  # 关注稳健 (A only)
        "error": None
    }
    
    if not radar_result.get("success"):
        result["error"] = radar_result.get("error")
        return result
    
    try:
        survey_codes = set(s["ts_code"] for s in radar_result.get("survey_stocks", []))
        inst_codes = set(s["ts_code"] for s in radar_result.get("top_inst_stocks", []))
        
        # 核心龙头: 调研热度高 + 机构抢筹
        core_codes = survey_codes & inst_codes
        
        # 构建股票池
        core_stocks = []
        attention_stocks = []
        
        # 创建快速查找表
        survey_map = {s["ts_code"]: s for s in radar_result.get("survey_stocks", [])}
        inst_map = {s["ts_code"]: s for s in radar_result.get("top_inst_stocks", [])}
        
        for ts_code in core_codes:
            stock_info = {
                "ts_code": ts_code,
                "name": survey_map.get(ts_code, {}).get("name", ""),
                "track_name": survey_map.get(ts_code, {}).get("track_name", ""),
                "survey_count": survey_map.get(ts_code, {}).get("survey_count", 0),
                "net_buy": inst_map.get(ts_code, {}).get("net_buy", 0),
                "label": "🌟 [机构抢筹·核心龙头]"
            }
            core_stocks.append(stock_info)
        
        # 关注稳健: 仅调研热度高
        attention_only_codes = survey_codes - core_codes
        for ts_code in attention_only_codes:
            stock_info = {
                "ts_code": ts_code,
                "name": survey_map.get(ts_code, {}).get("name", ""),
                "track_name": survey_map.get(ts_code, {}).get("track_name", ""),
                "survey_count": survey_map.get(ts_code, {}).get("survey_count", 0),
                "net_buy": 0,
                "label": "🏷️ [机构关注·趋势稳健]"
            }
            attention_stocks.append(stock_info)
        
        result["success"] = True
        result["core_stocks"] = sorted(core_stocks, key=lambda x: x.get("net_buy", 0), reverse=True)
        result["attention_stocks"] = sorted(attention_stocks, key=lambda x: x.get("survey_count", 0), reverse=True)
        
    except Exception as e:
        result["error"] = str(e)
        result["success"] = True
    
    return result


def _format_inst_track_report(
    trade_date: str,
    tracks_result: Dict[str, Any],
    radar_result: Dict[str, Any],
    pool_result: Dict[str, Any],
    survey_threshold: int,
    survey_days: int,
    top_inst_days: int
) -> str:
    """格式化机构抱团扫描报告"""
    
    # 格式化日期
    formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}" if len(trade_date) == 8 else trade_date
    
    lines = []
    lines.append("📊 机构抱团扫描报告 (Institutional Track)")
    lines.append("=" * 60)
    lines.append(f"📅 分析日期: {formatted_date}")
    lines.append("")
    
    # 模块1: 黄金赛道锁定
    lines.append("【1. 黄金赛道锁定】")
    lines.append("━" * 60)
    
    if tracks_result.get("success") and tracks_result.get("golden_tracks"):
        lines.append("🏆 机构趋势赛道 Top 3:")
        lines.append("")
        lines.append("| 排名 | 行业名称         | Alpha    | 机构偏好 |")
        lines.append("|-----|-----------------|----------|---------|")
        
        for i, track in enumerate(tracks_result["golden_tracks"][:3], 1):
            name = track.get("name", "")[:12]
            alpha = f"{track.get('alpha', 0):+.2f}%"
            preferred = "⭐ 重点" if track.get("is_preferred") else ""
            lines.append(f"| {i:<3} | {name:<15} | {alpha:<8} | {preferred:<7} |")
    else:
        lines.append(f"⚠️ 数据获取失败: {tracks_result.get('error', '未知错误')}")
    lines.append("")
    
    # 模块2: 机构雷达扫描
    lines.append("【2. 机构雷达扫描】")
    lines.append("━" * 60)
    
    # 调研热度
    lines.append(f"📡 调研热度 (近{survey_days}日调研 >= {survey_threshold}家):")
    if radar_result.get("survey_stocks"):
        lines.append("| 股票代码    | 股票名称   | 所属行业  | 调研家数 |")
        lines.append("|------------|-----------|----------|---------|")
        for stock in radar_result["survey_stocks"][:10]:
            ts_code = stock.get("ts_code", "")
            name = stock.get("name", "")[:8]
            track = stock.get("track_name", "")[:8]
            count = stock.get("survey_count", 0)
            lines.append(f"| {ts_code:<10} | {name:<9} | {track:<8} | {count:<7} |")
    else:
        lines.append("📌 无符合条件的调研热门股票")
    lines.append("")
    
    # 龙虎榜抢筹
    lines.append(f"📊 龙虎榜抢筹 (近{top_inst_days}日机构净买入):")
    if radar_result.get("top_inst_stocks"):
        lines.append("| 股票代码    | 股票名称   | 机构净买入(万) | 上榜日期  |")
        lines.append("|------------|-----------|--------------|----------|")
        for stock in radar_result["top_inst_stocks"][:10]:
            ts_code = stock.get("ts_code", "")
            name = stock.get("name", "")[:8]
            net_buy = f"{stock.get('net_buy', 0):,.0f}"
            date = stock.get("date", "")
            if len(date) == 8:
                date = f"{date[4:6]}-{date[6:8]}"
            lines.append(f"| {ts_code:<10} | {name:<9} | {net_buy:<12} | {date:<8} |")
    else:
        lines.append("📌 无符合条件的龙虎榜机构买入股票")
    lines.append("")
    
    # 模块3: 机构票池输出
    lines.append("【3. 机构票池输出】")
    lines.append("━" * 60)
    
    if pool_result.get("success"):
        # 核心龙头
        if pool_result.get("core_stocks"):
            lines.append("🌟 核心龙头 (调研热度高 + 机构抢筹):")
            lines.append("| 股票代码    | 股票名称   | 行业      | 调研数 | 净买入(万) |")
            lines.append("|------------|-----------|----------|-------|-----------|")
            for stock in pool_result["core_stocks"][:5]:
                ts_code = stock.get("ts_code", "")
                name = stock.get("name", "")[:8]
                track = stock.get("track_name", "")[:8]
                survey = stock.get("survey_count", 0)
                net_buy = f"{stock.get('net_buy', 0):,.0f}"
                lines.append(f"| {ts_code:<10} | {name:<9} | {track:<8} | {survey:<5} | {net_buy:<9} |")
            lines.append("")
        
        # 关注稳健
        if pool_result.get("attention_stocks"):
            lines.append("🏷️ 趋势稳健 (调研热度高):")
            lines.append("| 股票代码    | 股票名称   | 行业      | 调研数 |")
            lines.append("|------------|-----------|----------|-------|")
            for stock in pool_result["attention_stocks"][:10]:
                ts_code = stock.get("ts_code", "")
                name = stock.get("name", "")[:8]
                track = stock.get("track_name", "")[:8]
                survey = stock.get("survey_count", 0)
                lines.append(f"| {ts_code:<10} | {name:<9} | {track:<8} | {survey:<5} |")
        
        if not pool_result.get("core_stocks") and not pool_result.get("attention_stocks"):
            lines.append("📌 当前无符合条件的机构票池")
    else:
        lines.append(f"⚠️ 票池生成失败: {pool_result.get('error', '未知错误')}")
    
    lines.append("")
    lines.append("=" * 60)
    lines.append("📝 标签说明:")
    lines.append("  • 🌟 [机构抢筹·核心龙头]: 调研热度高 + 龙虎榜机构净买入")
    lines.append("  • 🏷️ [机构关注·趋势稳健]: 调研热度高")
    
    return "\n".join(lines)

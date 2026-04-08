"""外汇相关MCP工具"""
import tushare as ts
import pandas as pd
from typing import TYPE_CHECKING
from config.token_manager import get_tushare_token

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

from cache.cache_manager import cache_manager
from utils.common import format_date


def register_fx_tools(mcp: "FastMCP"):
    """注册外汇相关工具"""
    
    @mcp.tool()
    def get_fx_daily(
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取外汇日线行情数据
        
        参数:
            ts_code: 交易对代码（如：USDCNH.FXCM美元人民币，支持多个交易对同时提取，逗号分隔，如：USDCNH.FXCM,EURUSD.FXCM）
            trade_date: 交易日期（YYYYMMDD格式，如：20240101，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，如：20240101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20241231，需与start_date配合使用）
        
        注意：
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：获取外汇日线行情数据，包括买入价和卖出价
        
        常用交易对代码示例：
            - USDCNH.FXCM: 美元人民币
            - EURUSD.FXCM: 欧元美元
            - GBPUSD.FXCM: 英镑美元
            - USDJPY.FXCM: 美元日元
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code and not trade_date:
            return "请至少提供交易对代码(ts_code)或交易日期(trade_date)之一"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip() if ts_code else None
            trade_date = trade_date.strip() if trade_date else None
            start_date = start_date.strip() if start_date else None
            end_date = end_date.strip() if end_date else None
            
            if trade_date and (start_date or end_date):
                # 如果同时提供了trade_date和日期范围，优先使用trade_date
                start_date = None
                end_date = None
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'ts_code': ts_code or '',
                'trade_date': trade_date or '',
                'start_date': start_date or '',
                'end_date': end_date or ''
            }
            df = cache_manager.get_dataframe('fx_daily', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True  # 未找到数据，需要从API获取
            elif cache_manager.is_expired('fx_daily', **cache_params):
                need_update = True  # 数据过期，需要更新
            
            if need_update:
                # 过期后立即更新（同步）
                pro = ts.pro_api()
                params = {}
                
                if ts_code:
                    params['ts_code'] = ts_code
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                df = pro.fx_daily(**params)
                
                # 保存到缓存（创建新版本）
                if not df.empty:
                    cache_manager.set('fx_daily', df, **cache_params)
            
            if df.empty:
                if ts_code:
                    fx_info = f"交易对 {ts_code}"
                else:
                    fx_info = "外汇"
                
                if trade_date:
                    date_info = f"日期 {trade_date}"
                elif start_date or end_date:
                    if start_date and end_date:
                        date_info = f"日期范围 {start_date} 至 {end_date}"
                    elif start_date:
                        date_info = f"日期范围从 {start_date} 开始"
                    else:
                        date_info = f"日期范围到 {end_date} 结束"
                else:
                    date_info = "最近数据"
                return f"未找到 {fx_info} 在 {date_info} 的日线行情数据，请检查参数是否正确"
            
            # 格式化输出
            return format_fx_daily_data(df, ts_code or "")
            
        except Exception as e:
            return f"查询失败：{str(e)}"


def format_fx_daily_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化外汇日线行情数据输出
    
    参数:
        df: 外汇日线行情数据DataFrame
        ts_code: 交易对代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的外汇日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 如果查询的是单个交易对或多个交易对
    if ts_code:
        # 按交易对代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            fx_df = df[df['ts_code'] == code]
            if not fx_df.empty:
                result.append(format_single_fx_daily(fx_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有交易对
        # 按日期分组
        dates = df['trade_date'].unique()
        for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10个交易日
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                result.append(f"📅 交易日期: {format_date(date)}")
                result.append("=" * 120)
                result.append(f"{'交易对':<20} {'买入收盘':<12} {'卖出收盘':<12} {'买入最高':<12} {'买入最低':<12} {'报价笔数':<12}")
                result.append("-" * 120)
                for _, row in date_df.iterrows():
                    ts_code_str = str(row.get('ts_code', '-'))
                    bid_close = f"{row.get('bid_close', 0):.4f}" if pd.notna(row.get('bid_close')) else "-"
                    ask_close = f"{row.get('ask_close', 0):.4f}" if pd.notna(row.get('ask_close')) else "-"
                    bid_high = f"{row.get('bid_high', 0):.4f}" if pd.notna(row.get('bid_high')) else "-"
                    bid_low = f"{row.get('bid_low', 0):.4f}" if pd.notna(row.get('bid_low')) else "-"
                    tick_qty = f"{row.get('tick_qty', 0):.0f}" if pd.notna(row.get('tick_qty')) else "-"
                    result.append(f"{ts_code_str:<20} {bid_close:<12} {ask_close:<12} {bid_high:<12} {bid_low:<12} {tick_qty:<12}")
                result.append("")
    
    return "\n".join(result)


def format_single_fx_daily(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个交易对的日线行情数据
    
    参数:
        df: 单个交易对的日线行情数据DataFrame
        ts_code: 交易对代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"💱 {ts_code} 外汇日线行情")
    result.append("=" * 120)
    result.append("")
    
    # 显示最近的数据（最多20条）
    display_count = min(20, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'买入开盘':<12} {'买入收盘':<12} {'买入最高':<12} {'买入最低':<12} {'卖出开盘':<12} {'卖出收盘':<12} {'卖出最高':<12} {'卖出最低':<12} {'报价笔数':<12}")
    result.append("-" * 120)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(str(row.get('trade_date', '-')))
        bid_open = f"{row.get('bid_open', 0):.4f}" if pd.notna(row.get('bid_open')) else "-"
        bid_close = f"{row.get('bid_close', 0):.4f}" if pd.notna(row.get('bid_close')) else "-"
        bid_high = f"{row.get('bid_high', 0):.4f}" if pd.notna(row.get('bid_high')) else "-"
        bid_low = f"{row.get('bid_low', 0):.4f}" if pd.notna(row.get('bid_low')) else "-"
        ask_open = f"{row.get('ask_open', 0):.4f}" if pd.notna(row.get('ask_open')) else "-"
        ask_close = f"{row.get('ask_close', 0):.4f}" if pd.notna(row.get('ask_close')) else "-"
        ask_high = f"{row.get('ask_high', 0):.4f}" if pd.notna(row.get('ask_high')) else "-"
        ask_low = f"{row.get('ask_low', 0):.4f}" if pd.notna(row.get('ask_low')) else "-"
        tick_qty = f"{row.get('tick_qty', 0):.0f}" if pd.notna(row.get('tick_qty')) else "-"
        
        result.append(f"{trade_date:<12} {bid_open:<12} {bid_close:<12} {bid_high:<12} {bid_low:<12} {ask_open:<12} {ask_close:<12} {ask_high:<12} {ask_low:<12} {tick_qty:<12}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 120)
        trade_date_str = str(latest.get('trade_date', '-'))
        result.append(f"交易日期: {format_date(trade_date_str)}")
        result.append("")
        result.append("买入价（Bid）：")
        result.append(f"  开盘: {latest.get('bid_open', 0):.4f}" if pd.notna(latest.get('bid_open')) else "  开盘: -")
        result.append(f"  收盘: {latest.get('bid_close', 0):.4f}" if pd.notna(latest.get('bid_close')) else "  收盘: -")
        result.append(f"  最高: {latest.get('bid_high', 0):.4f}" if pd.notna(latest.get('bid_high')) else "  最高: -")
        result.append(f"  最低: {latest.get('bid_low', 0):.4f}" if pd.notna(latest.get('bid_low')) else "  最低: -")
        result.append("")
        result.append("卖出价（Ask）：")
        result.append(f"  开盘: {latest.get('ask_open', 0):.4f}" if pd.notna(latest.get('ask_open')) else "  开盘: -")
        result.append(f"  收盘: {latest.get('ask_close', 0):.4f}" if pd.notna(latest.get('ask_close')) else "  收盘: -")
        result.append(f"  最高: {latest.get('ask_high', 0):.4f}" if pd.notna(latest.get('ask_high')) else "  最高: -")
        result.append(f"  最低: {latest.get('ask_low', 0):.4f}" if pd.notna(latest.get('ask_low')) else "  最低: -")
        result.append("")
        if pd.notna(latest.get('tick_qty')):
            result.append(f"报价笔数: {latest.get('tick_qty', 0):.0f}")
        if pd.notna(latest.get('exchange')):
            result.append(f"交易商: {latest.get('exchange', '-')}")
    
    return "\n".join(result)



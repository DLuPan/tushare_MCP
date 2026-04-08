"""期货相关MCP工具"""
import tushare as ts
import pandas as pd
from typing import TYPE_CHECKING
from config.token_manager import get_tushare_token

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

from cache.cache_manager import cache_manager
from utils.common import format_date


def register_futures_tools(mcp: "FastMCP"):
    """注册期货相关工具"""
    
    @mcp.tool()
    def get_fut_basic(
        exchange: str = "",
        fut_type: str = "",
        fut_code: str = "",
        list_date: str = ""
    ) -> str:
        """
        获取期货合约基本信息
        
        参数:
            exchange: 交易所代码（必填）
                - CFFEX: 中金所
                - DCE: 大商所
                - CZCE: 郑商所
                - SHFE: 上期所
                - INE: 上海国际能源交易中心
                - GFEX: 广州期货交易所
            fut_type: 合约类型（可选）
                - 1: 普通合约
                - 2: 主力与连续合约
                - 留空: 默认取全部
            fut_code: 标准合约代码（可选，如：白银AG、AP鲜苹果等）
            list_date: 上市开始日期（可选，YYYYMMDD格式，从某日开始以来所有合约）
        
        返回:
            包含期货合约基本信息的格式化字符串
        
        说明:
            - 数据来源：Tushare
            - 显示合约代码、交易标识、交易市场、中文简称、合约产品代码等信息
            - 显示合约乘数、交易计量单位、交易单位、报价单位等交易信息
            - 显示上市日期、最后交易日期、交割月份、最后交割日等时间信息
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：exchange是必填参数
        if not exchange:
            return "请提供交易所代码(exchange)，可选值：CFFEX(中金所)、DCE(大商所)、CZCE(郑商所)、SHFE(上期所)、INE(上海国际能源交易中心)、GFEX(广州期货交易所)"
        
        # 验证交易所代码
        valid_exchanges = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX']
        if exchange.upper() not in valid_exchanges:
            return f"无效的交易所代码: {exchange}\n支持的交易所代码：{', '.join(valid_exchanges)}"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {
                'exchange': exchange.upper()
            }
            if fut_type:
                params['fut_type'] = fut_type
            if fut_code:
                params['fut_code'] = fut_code
            if list_date:
                params['list_date'] = list_date
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'exchange': exchange.upper(),
                'fut_type': fut_type or '',
                'fut_code': fut_code or '',
                'list_date': list_date or ''
            }
            df = cache_manager.get_dataframe('fut_basic', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('fut_basic', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                df = pro.fut_basic(**params)
                
                # 保存到缓存（创建新版本）
                if not df.empty:
                    cache_manager.set('fut_basic', df, **cache_params)
            
            if df.empty:
                param_info = []
                param_info.append(f"交易所: {exchange}")
                if fut_type:
                    param_info.append(f"合约类型: {fut_type}")
                if fut_code:
                    param_info.append(f"合约代码: {fut_code}")
                if list_date:
                    param_info.append(f"上市日期: {list_date}")
                
                return f"未找到符合条件的期货合约信息\n查询条件: {', '.join(param_info)}"
            
            # 格式化输出
            return format_fut_basic_data(df, exchange.upper(), fut_code or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_nh_index(
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取南华期货指数日线行情数据
        
        参数:
            ts_code: 指数代码（如：CU.NH南华沪铜指数、NHCI.NH南华商品指数等，支持多个，逗号分隔）
            trade_date: 交易日期（YYYYMMDD格式，如：20181130，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，需与start_date配合使用）
        
        返回:
            包含南华期货指数日线行情数据的格式化字符串
        
        说明:
            - 数据来源：Tushare index_daily接口
            - 支持按指数代码、交易日期、日期范围查询
            - 显示开盘、最高、最低、收盘、涨跌点、涨跌幅、成交量、成交额等行情数据
            - 权限要求：2000积分
            - 常用指数代码：
              * NHAI.NH: 南华农产品指数
              * NHCI.NH: 南华商品指数
              * NHECI.NH: 南华能化指数
              * NHFI.NH: 南华黑色指数
              * NHII.NH: 南华工业品指数
              * NHMI.NH: 南华金属指数
              * CU.NH: 南华沪铜指数
              * AU.NH: 南华沪黄金指数
              * PB.NH: 南华沪铅指数
              * NI.NH: 南华沪镍指数
              * SN.NH: 南华沪锡指数
              * ZN.NH: 南华沪锌指数
              * RB.NH: 南华螺纹钢指数
              * WR.NH: 南华线材指数
              * HC.NH: 南华热轧卷板指数
              * SS.NH: 南华不锈钢指数
              * 等等（详见文档）
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not trade_date and not start_date and not end_date:
            return "请至少提供以下参数之一：指数代码(ts_code)、交易日期(trade_date)或日期范围(start_date/end_date)"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if trade_date:
                params['trade_date'] = trade_date
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            
            # 如果同时提供了trade_date和日期范围，优先使用trade_date
            if trade_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'ts_code': ts_code or '',
                'trade_date': trade_date or '',
                'start_date': start_date or '',
                'end_date': end_date or ''
            }
            df = cache_manager.get_dataframe('nh_index', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('nh_index', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                # 使用index_daily接口获取南华期货指数行情（南华指数以.NH结尾）
                try:
                    df = pro.index_daily(**params)
                    
                    # 如果提供了ts_code，确保只返回.NH结尾的指数
                    # 如果没有提供ts_code，过滤出所有.NH结尾的指数
                    if not df.empty:
                        if ts_code:
                            # 如果提供了ts_code，确保都是.NH结尾的
                            codes = [code.strip() for code in ts_code.split(',')]
                            df = df[df['ts_code'].isin(codes)]
                        else:
                            # 如果没有提供ts_code，只返回.NH结尾的指数
                            df = df[df['ts_code'].str.endswith('.NH', na=False)]
                    
                    # 保存到缓存（创建新版本）
                    if not df.empty:
                        cache_manager.set('nh_index', df, **cache_params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：index_daily\n\n可能的原因：\n1. Tushare token是否有效\n2. 账户积分是否达到2000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日\n\n建议：\n- 请查看Tushare文档确认index_daily接口是否支持南华指数\n- 检查Tushare账户积分是否足够"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户积分是否达到2000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日"
            
            if df is None or df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"指数代码: {ts_code}")
                if trade_date:
                    param_info.append(f"交易日期: {trade_date}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                
                return f"未找到符合条件的南华期货指数行情数据\n查询条件: {', '.join(param_info)}"
            
            # 按交易日期排序（最新的在前）
            if 'trade_date' in df.columns:
                df = df.sort_values('trade_date', ascending=False)
            
            # 格式化输出
            return format_nh_index_data(df, ts_code or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_fut_holding(
        trade_date: str = "",
        symbol: str = "",
        start_date: str = "",
        end_date: str = "",
        exchange: str = ""
    ) -> str:
        """
        获取期货每日持仓排名数据
        
        参数:
            trade_date: 交易日期（YYYYMMDD格式，如：20181113，查询指定日期的数据）
            symbol: 合约或产品代码（如：C1905、C等，可选）
            start_date: 开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，需与start_date配合使用）
            exchange: 交易所代码（可选）
                - CFFEX: 中金所
                - DCE: 大商所
                - CZCE: 郑商所
                - SHFE: 上期所
                - INE: 上海国际能源交易中心
                - GFEX: 广州期货交易所
        
        返回:
            包含期货持仓排名数据的格式化字符串
        
        说明:
            - 数据来源：Tushare fut_holding接口
            - 支持按交易日期、合约代码、交易所筛选
            - 显示期货公司会员的成交量、持仓量等数据
            - 权限要求：2000积分
            - 限量：单次最大可调取5000条数据
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not trade_date and not start_date and not end_date:
            return "请至少提供以下参数之一：交易日期(trade_date)或日期范围(start_date/end_date)"
        
        # 如果提供了日期范围，必须同时提供start_date和end_date
        if (start_date and not end_date) or (end_date and not start_date):
            return "如果使用日期范围查询，请同时提供start_date和end_date"
        
        # 验证交易所代码（如果提供）
        if exchange:
            valid_exchanges = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX']
            if exchange.upper() not in valid_exchanges:
                return f"无效的交易所代码: {exchange}\n支持的交易所代码：{', '.join(valid_exchanges)}"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if trade_date:
                params['trade_date'] = trade_date
            if symbol:
                params['symbol'] = symbol
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            if exchange:
                params['exchange'] = exchange.upper()
            
            # 如果同时提供了trade_date和日期范围，优先使用trade_date
            if trade_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'trade_date': trade_date or '',
                'symbol': symbol or '',
                'start_date': start_date or '',
                'end_date': end_date or '',
                'exchange': exchange.upper() if exchange else ''
            }
            df = cache_manager.get_dataframe('fut_holding', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('fut_holding', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                try:
                    df = pro.fut_holding(**params)
                    
                    # 保存到缓存（创建新版本）
                    if not df.empty:
                        cache_manager.set('fut_holding', df, **cache_params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：fut_holding\n\n可能的原因：\n1. Tushare token是否有效\n2. 账户积分是否达到2000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日\n\n建议：\n- 请查看Tushare文档确认fut_holding接口是否可用\n- 检查Tushare账户积分是否足够"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户积分是否达到2000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日"
            
            if df is None or df.empty:
                param_info = []
                if trade_date:
                    param_info.append(f"交易日期: {trade_date}")
                if symbol:
                    param_info.append(f"合约代码: {symbol}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                if exchange:
                    param_info.append(f"交易所: {exchange}")
                
                return f"未找到符合条件的期货持仓排名数据\n查询条件: {', '.join(param_info)}"
            
            # 按交易日期排序（最新的在前）
            if 'trade_date' in df.columns:
                df = df.sort_values('trade_date', ascending=False)
            
            # 格式化输出
            return format_fut_holding_data(df, trade_date or start_date or "", symbol or "", exchange or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_fut_wsr(
        trade_date: str = "",
        symbol: str = "",
        start_date: str = "",
        end_date: str = "",
        exchange: str = ""
    ) -> str:
        """
        获取期货仓单日报数据
        
        参数:
            trade_date: 交易日期（YYYYMMDD格式，如：20181113，查询指定日期的数据）
            symbol: 产品代码（如：ZN锌、CU铜等，可选）
            start_date: 开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，需与start_date配合使用）
            exchange: 交易所代码（可选）
                - CFFEX: 中金所
                - DCE: 大商所
                - CZCE: 郑商所
                - SHFE: 上期所
                - INE: 上海国际能源交易中心
                - GFEX: 广州期货交易所
        
        返回:
            包含期货仓单日报数据的格式化字符串
        
        说明:
            - 数据来源：Tushare fut_wsr接口
            - 支持按交易日期、产品代码、交易所筛选
            - 显示各仓库/厂库的仓单变化情况
            - 权限要求：2000积分
            - 限量：单次最大可调取1000条数据
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not trade_date and not start_date and not end_date:
            return "请至少提供以下参数之一：交易日期(trade_date)或日期范围(start_date/end_date)"
        
        # 如果提供了日期范围，必须同时提供start_date和end_date
        if (start_date and not end_date) or (end_date and not start_date):
            return "如果使用日期范围查询，请同时提供start_date和end_date"
        
        # 验证交易所代码（如果提供）
        if exchange:
            valid_exchanges = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX']
            if exchange.upper() not in valid_exchanges:
                return f"无效的交易所代码: {exchange}\n支持的交易所代码：{', '.join(valid_exchanges)}"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if trade_date:
                params['trade_date'] = trade_date
            if symbol:
                params['symbol'] = symbol.upper()
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            if exchange:
                params['exchange'] = exchange.upper()
            
            # 如果同时提供了trade_date和日期范围，优先使用trade_date
            if trade_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'trade_date': trade_date or '',
                'symbol': symbol.upper() if symbol else '',
                'start_date': start_date or '',
                'end_date': end_date or '',
                'exchange': exchange.upper() if exchange else ''
            }
            df = cache_manager.get_dataframe('fut_wsr', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('fut_wsr', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                try:
                    df = pro.fut_wsr(**params)
                    
                    # 保存到缓存（创建新版本）
                    if not df.empty:
                        cache_manager.set('fut_wsr', df, **cache_params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：fut_wsr\n\n可能的原因：\n1. Tushare token是否有效\n2. 账户积分是否达到2000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日\n\n建议：\n- 请查看Tushare文档确认fut_wsr接口是否可用\n- 检查Tushare账户积分是否足够"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户积分是否达到2000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日"
            
            if df is None or df.empty:
                param_info = []
                if trade_date:
                    param_info.append(f"交易日期: {trade_date}")
                if symbol:
                    param_info.append(f"产品代码: {symbol}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                if exchange:
                    param_info.append(f"交易所: {exchange}")
                
                return f"未找到符合条件的期货仓单日报数据\n查询条件: {', '.join(param_info)}"
            
            # 按交易日期排序（最新的在前）
            if 'trade_date' in df.columns:
                df = df.sort_values('trade_date', ascending=False)
            
            # 格式化输出
            return format_fut_wsr_data(df, trade_date or start_date or "", symbol or "", exchange or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_fut_min(
        ts_code: str = "",
        freq: str = "1MIN",
        date_str: str = ""
    ) -> str:
        """
        获取期货实时分钟行情数据
        
        参数:
            ts_code: 期货合约代码（必填，如：CU2501.SHF，支持多个合约，逗号分隔，如：CU2501.SHF,CU2502.SHF）
            freq: 分钟频度（必填，默认1MIN）
                - 1MIN: 1分钟
                - 5MIN: 5分钟
                - 15MIN: 15分钟
                - 30MIN: 30分钟
                - 60MIN: 60分钟
            date_str: 回放日期（可选，格式：YYYY-MM-DD，默认为交易当日，支持回溯一天）
                如果提供此参数，将使用rt_fut_min_daily接口获取当日开市以来所有历史分钟数据
        
        返回:
            包含期货实时分钟行情数据的格式化字符串
        
        说明:
            - 数据来源：Tushare rt_fut_min接口（实时）或rt_fut_min_daily接口（历史回放）
            - 支持1min/5min/15min/30min/60min行情
            - 显示开盘、最高、最低、收盘、成交量、成交金额、持仓量等数据
            - 权限要求：需单独开权限，正式权限请参阅权限说明
            - 限量：每分钟可以请求500次，支持多个合约同时提取
            - 注意：如果需要主力合约分钟，请先通过主力mapping接口获取对应的合约代码
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code:
            return "请提供期货合约代码(ts_code)，如：CU2501.SHF，支持多个合约（逗号分隔）"
        
        # 验证freq参数
        valid_freqs = ['1MIN', '5MIN', '15MIN', '30MIN', '60MIN']
        if freq.upper() not in valid_freqs:
            return f"无效的分钟频度: {freq}\n支持的频度：{', '.join(valid_freqs)}"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {
                'ts_code': ts_code,
                'freq': freq.upper()
            }
            
            # 判断使用哪个接口
            use_daily = bool(date_str)
            if use_daily:
                params['date_str'] = date_str
                # 如果提供了date_str，使用rt_fut_min_daily接口（只支持单个合约）
                codes = [code.strip() for code in ts_code.split(',')]
                if len(codes) > 1:
                    return "rt_fut_min_daily接口只支持一次一个合约的回放，请提供单个合约代码"
            
            # 尝试从缓存获取（实时数据不缓存，历史回放数据可以缓存）
            df = None
            if use_daily:
                cache_params = {
                    'ts_code': ts_code,
                    'freq': freq.upper(),
                    'date_str': date_str
                }
                df = cache_manager.get_dataframe('fut_min_daily', **cache_params)
                
                # 检查是否需要更新（过期后立即更新）
                need_update = False
                if df is None:
                    need_update = True
                elif cache_manager.is_expired('fut_min_daily', **cache_params):
                    need_update = True
                
                if need_update:
                    # 使用rt_fut_min_daily接口获取历史分钟数据
                    try:
                        df = pro.rt_fut_min_daily(**params)
                        
                        # 保存到缓存（创建新版本）
                        if not df.empty:
                            cache_manager.set('fut_min_daily', df, **cache_params)
                    except Exception as api_error:
                        error_msg = str(api_error)
                        # 检查是否是接口名错误
                        if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                            return f"API接口调用失败：{error_msg}\n\n已使用接口：rt_fut_min_daily\n\n可能的原因：\n1. Tushare token是否有效\n2. 是否已开通期货实时分钟行情权限\n3. 网络连接是否正常\n4. 合约代码格式是否正确（如：CU2501.SHF）\n\n建议：\n- 请查看Tushare文档确认rt_fut_min_daily接口是否可用\n- 检查是否已开通相应权限"
                        else:
                            return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 是否已开通期货实时分钟行情权限\n3. 网络连接是否正常\n4. 合约代码格式是否正确"
            else:
                # 使用rt_fut_min接口获取实时分钟数据（不缓存）
                try:
                    df = pro.rt_fut_min(**params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：rt_fut_min\n\n可能的原因：\n1. Tushare token是否有效\n2. 是否已开通期货实时分钟行情权限\n3. 网络连接是否正常\n4. 合约代码格式是否正确（如：CU2501.SHF）\n\n建议：\n- 请查看Tushare文档确认rt_fut_min接口是否可用\n- 检查是否已开通相应权限"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 是否已开通期货实时分钟行情权限\n3. 网络连接是否正常\n4. 合约代码格式是否正确"
            
            if df is None or df.empty:
                param_info = []
                param_info.append(f"合约代码: {ts_code}")
                param_info.append(f"分钟频度: {freq}")
                if date_str:
                    param_info.append(f"回放日期: {date_str}")
                
                return f"未找到符合条件的期货分钟行情数据\n查询条件: {', '.join(param_info)}"
            
            # 按时间排序（最新的在前）
            if 'time' in df.columns:
                df = df.sort_values('time', ascending=False)
            
            # 格式化输出
            return format_fut_min_data(df, ts_code, freq.upper(), date_str)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"


def format_fut_basic_data(df: pd.DataFrame, exchange: str, fut_code: str = "") -> str:
    """
    格式化期货合约基本信息输出
    
    参数:
        df: 期货合约基本信息DataFrame
        exchange: 交易所代码
        fut_code: 合约代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的期货合约信息"
    
    # 交易所名称映射
    exchange_names = {
        'CFFEX': '中金所',
        'DCE': '大商所',
        'CZCE': '郑商所',
        'SHFE': '上期所',
        'INE': '上海国际能源交易中心',
        'GFEX': '广州期货交易所'
    }
    exchange_name = exchange_names.get(exchange, exchange)
    
    result = []
    result.append(f"📊 {exchange_name}({exchange}) 期货合约基本信息")
    result.append("=" * 180)
    result.append("")
    
    # 如果查询的是单个合约代码
    if fut_code:
        code_df = df[df['fut_code'] == fut_code]
        if not code_df.empty:
            result.append(format_single_fut_basic(code_df, fut_code))
            return "\n".join(result)
    
    # 按合约代码分组显示
    if 'fut_code' in df.columns:
        fut_codes = sorted(df['fut_code'].unique())
        result.append(f"共找到 {len(df)} 个合约，涉及 {len(fut_codes)} 个合约品种")
        result.append("")
        
        # 显示每个合约品种的合约列表
        for code in fut_codes[:20]:  # 最多显示前20个品种
            code_df = df[df['fut_code'] == code]
            if not code_df.empty:
                result.append(f"📦 {code} ({len(code_df)} 个合约)")
                result.append("-" * 180)
                result.append(f"{'合约代码':<20} {'交易标识':<15} {'中文简称':<20} {'上市日期':<12} {'最后交易日期':<14} {'交割月份':<12} {'最后交割日':<12} {'交易单位':<12} {'报价单位':<15} {'合约乘数':<12}")
                result.append("-" * 180)
                
                # 按上市日期排序
                if 'list_date' in code_df.columns:
                    code_df = code_df.sort_values('list_date', ascending=False)
                
                for _, row in code_df.head(10).iterrows():  # 每个品种最多显示10个合约
                    ts_code = str(row.get('ts_code', '-'))[:18]
                    symbol = str(row.get('symbol', '-'))[:13]
                    name = str(row.get('name', '-'))[:18]
                    list_date = format_date(str(row.get('list_date', '-'))) if pd.notna(row.get('list_date')) else "-"
                    delist_date = format_date(str(row.get('delist_date', '-'))) if pd.notna(row.get('delist_date')) else "-"
                    d_month = str(row.get('d_month', '-'))[:10]
                    last_ddate = format_date(str(row.get('last_ddate', '-'))) if pd.notna(row.get('last_ddate')) else "-"
                    per_unit = f"{row.get('per_unit', 0):.0f}" if pd.notna(row.get('per_unit')) else "-"
                    quote_unit = str(row.get('quote_unit', '-'))[:13]
                    multiplier = f"{row.get('multiplier', 0):.0f}" if pd.notna(row.get('multiplier')) else "-"
                    
                    result.append(f"{ts_code:<20} {symbol:<15} {name:<20} {list_date:<12} {delist_date:<14} {d_month:<12} {last_ddate:<12} {per_unit:<12} {quote_unit:<15} {multiplier:<12}")
                
                if len(code_df) > 10:
                    result.append(f"  ... 还有 {len(code_df) - 10} 个合约未显示")
                result.append("")
        
        if len(fut_codes) > 20:
            result.append(f"  ... 还有 {len(fut_codes) - 20} 个合约品种未显示")
    else:
        # 如果没有fut_code字段，直接显示所有合约
        result.append(f"共找到 {len(df)} 个合约")
        result.append("")
        result.append(f"{'合约代码':<20} {'交易标识':<15} {'中文简称':<20} {'上市日期':<12} {'最后交易日期':<14} {'交割月份':<12} {'交易单位':<12} {'报价单位':<15}")
        result.append("-" * 180)
        
        # 按上市日期排序
        if 'list_date' in df.columns:
            df = df.sort_values('list_date', ascending=False)
        
        display_count = min(50, len(df))
        for _, row in df.head(display_count).iterrows():
            ts_code = str(row.get('ts_code', '-'))[:18]
            symbol = str(row.get('symbol', '-'))[:13]
            name = str(row.get('name', '-'))[:18]
            list_date = format_date(str(row.get('list_date', '-'))) if pd.notna(row.get('list_date')) else "-"
            delist_date = format_date(str(row.get('delist_date', '-'))) if pd.notna(row.get('delist_date')) else "-"
            d_month = str(row.get('d_month', '-'))[:10]
            per_unit = f"{row.get('per_unit', 0):.0f}" if pd.notna(row.get('per_unit')) else "-"
            quote_unit = str(row.get('quote_unit', '-'))[:13]
            
            result.append(f"{ts_code:<20} {symbol:<15} {name:<20} {list_date:<12} {delist_date:<14} {d_month:<12} {per_unit:<12} {quote_unit:<15}")
        
        if len(df) > display_count:
            result.append("")
            result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare")
    result.append("  - 合约类型：1=普通合约，2=主力与连续合约")
    result.append("  - 交易单位：每手合约的交易单位")
    result.append("  - 合约乘数：只适用于国债期货、指数期货")
    
    return "\n".join(result)


def format_single_fut_basic(df: pd.DataFrame, fut_code: str) -> str:
    """
    格式化单个合约品种的详细信息
    
    参数:
        df: 单个合约品种的DataFrame
        fut_code: 合约代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {fut_code} 的合约信息"
    
    result = []
    result.append(f"📦 {fut_code} 期货合约详细信息")
    result.append("=" * 180)
    result.append("")
    
    # 按上市日期排序
    if 'list_date' in df.columns:
        df = df.sort_values('list_date', ascending=False)
    
    result.append(f"共找到 {len(df)} 个合约")
    result.append("")
    result.append(f"{'合约代码':<20} {'交易标识':<15} {'中文简称':<20} {'上市日期':<12} {'最后交易日期':<14} {'交割月份':<12} {'最后交割日':<12} {'交易单位':<12} {'报价单位':<15} {'合约乘数':<12} {'交割方式':<20}")
    result.append("-" * 180)
    
    for _, row in df.iterrows():
        ts_code = str(row.get('ts_code', '-'))[:18]
        symbol = str(row.get('symbol', '-'))[:13]
        name = str(row.get('name', '-'))[:18]
        list_date = format_date(str(row.get('list_date', '-'))) if pd.notna(row.get('list_date')) else "-"
        delist_date = format_date(str(row.get('delist_date', '-'))) if pd.notna(row.get('delist_date')) else "-"
        d_month = str(row.get('d_month', '-'))[:10]
        last_ddate = format_date(str(row.get('last_ddate', '-'))) if pd.notna(row.get('last_ddate')) else "-"
        per_unit = f"{row.get('per_unit', 0):.0f}" if pd.notna(row.get('per_unit')) else "-"
        quote_unit = str(row.get('quote_unit', '-'))[:13]
        multiplier = f"{row.get('multiplier', 0):.0f}" if pd.notna(row.get('multiplier')) else "-"
        d_mode_desc = str(row.get('d_mode_desc', '-'))[:18]
        
        result.append(f"{ts_code:<20} {symbol:<15} {name:<20} {list_date:<12} {delist_date:<14} {d_month:<12} {last_ddate:<12} {per_unit:<12} {quote_unit:<15} {multiplier:<12} {d_mode_desc:<20}")
    
    # 显示最新合约的详细信息
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新合约详细信息：")
        result.append("-" * 180)
        result.append(f"合约代码: {latest.get('ts_code', '-')}")
        result.append(f"交易标识: {latest.get('symbol', '-')}")
        result.append(f"中文简称: {latest.get('name', '-')}")
        result.append(f"交易市场: {latest.get('exchange', '-')}")
        result.append(f"合约产品代码: {latest.get('fut_code', '-')}")
        if pd.notna(latest.get('multiplier')):
            result.append(f"合约乘数: {latest.get('multiplier', 0):.0f}")
        result.append(f"交易计量单位: {latest.get('trade_unit', '-')}")
        if pd.notna(latest.get('per_unit')):
            result.append(f"交易单位(每手): {latest.get('per_unit', 0):.0f}")
        result.append(f"报价单位: {latest.get('quote_unit', '-')}")
        if pd.notna(latest.get('quote_unit_desc')):
            result.append(f"最小报价单位说明: {latest.get('quote_unit_desc', '-')}")
        if pd.notna(latest.get('d_mode_desc')):
            result.append(f"交割方式说明: {latest.get('d_mode_desc', '-')}")
        if pd.notna(latest.get('list_date')):
            result.append(f"上市日期: {format_date(str(latest.get('list_date', '-')))}")
        if pd.notna(latest.get('delist_date')):
            result.append(f"最后交易日期: {format_date(str(latest.get('delist_date', '-')))}")
        if pd.notna(latest.get('d_month')):
            result.append(f"交割月份: {latest.get('d_month', '-')}")
        if pd.notna(latest.get('last_ddate')):
            result.append(f"最后交割日: {format_date(str(latest.get('last_ddate', '-')))}")
        if pd.notna(latest.get('trade_time_desc')):
            result.append(f"交易时间说明: {latest.get('trade_time_desc', '-')}")
    
    return "\n".join(result)


def format_nh_index_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化南华期货指数行情数据输出
    
    参数:
        df: 南华期货指数行情数据DataFrame
        ts_code: 指数代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的南华期货指数行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 如果查询的是单个指数或多个指数
    if ts_code:
        # 按指数代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            index_df = df[df['ts_code'] == code]
            if not index_df.empty:
                result.append(format_single_nh_index(index_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 如果有多个交易日期，按日期分组显示
        if 'trade_date' in df.columns and len(df['trade_date'].unique()) > 1:
            dates = sorted(df['trade_date'].unique(), reverse=True)
            for date in dates[:10]:  # 最多显示最近10个交易日
                date_df = df[df['trade_date'] == date]
                if not date_df.empty:
                    result.append(f"📅 交易日期: {format_date(date)}")
                    result.append("=" * 140)
                    result.append(f"{'指数代码':<20} {'开盘':<12} {'最高':<12} {'最低':<12} {'收盘':<12} {'涨跌点':<12} {'涨跌幅':<12} {'成交量(手)':<15} {'成交额(千元)':<15}")
                    result.append("-" * 140)
                    
                    for _, row in date_df.iterrows():
                        code = str(row['ts_code']) if 'ts_code' in row and pd.notna(row['ts_code']) else "-"
                        open_price = f"{row['open']:.3f}" if 'open' in row and pd.notna(row['open']) else "-"
                        high = f"{row['high']:.3f}" if 'high' in row and pd.notna(row['high']) else "-"
                        low = f"{row['low']:.3f}" if 'low' in row and pd.notna(row['low']) else "-"
                        close = f"{row['close']:.3f}" if 'close' in row and pd.notna(row['close']) else "-"
                        change = f"{row['change']:+.3f}" if 'change' in row and pd.notna(row['change']) else "-"
                        pct_chg = f"{row['pct_chg']:+.2f}%" if 'pct_chg' in row and pd.notna(row['pct_chg']) else "-"
                        vol = f"{row['vol']:.0f}" if 'vol' in row and pd.notna(row['vol']) else "-"
                        amount = f"{row['amount']:.2f}" if 'amount' in row and pd.notna(row['amount']) else "-"
                        
                        result.append(f"{code:<20} {open_price:<12} {high:<12} {low:<12} {close:<12} {change:<12} {pct_chg:<12} {vol:<15} {amount:<15}")
                    result.append("")
        else:
            # 单个日期或单个指数，使用详细格式
            if ts_code and len(df['ts_code'].unique()) == 1:
                result.append(format_single_nh_index(df, df['ts_code'].iloc[0]))
            else:
                # 显示所有指数
                result.append("📊 南华期货指数行情数据")
                result.append("=" * 140)
                result.append(f"{'指数代码':<20} {'交易日期':<12} {'开盘':<12} {'最高':<12} {'最低':<12} {'收盘':<12} {'涨跌点':<12} {'涨跌幅':<12} {'成交量(手)':<15} {'成交额(千元)':<15}")
                result.append("-" * 140)
                
                for _, row in df.iterrows():
                    code = str(row['ts_code']) if 'ts_code' in row and pd.notna(row['ts_code']) else "-"
                    trade_date = format_date(str(row['trade_date'])) if 'trade_date' in row and pd.notna(row['trade_date']) else "-"
                    open_price = f"{row['open']:.3f}" if 'open' in row and pd.notna(row['open']) else "-"
                    high = f"{row['high']:.3f}" if 'high' in row and pd.notna(row['high']) else "-"
                    low = f"{row['low']:.3f}" if 'low' in row and pd.notna(row['low']) else "-"
                    close = f"{row['close']:.3f}" if 'close' in row and pd.notna(row['close']) else "-"
                    change = f"{row['change']:+.3f}" if 'change' in row and pd.notna(row['change']) else "-"
                    pct_chg = f"{row['pct_chg']:+.2f}%" if 'pct_chg' in row and pd.notna(row['pct_chg']) else "-"
                    vol = f"{row['vol']:.0f}" if 'vol' in row and pd.notna(row['vol']) else "-"
                    amount = f"{row['amount']:.2f}" if 'amount' in row and pd.notna(row['amount']) else "-"
                    
                    result.append(f"{code:<20} {trade_date:<12} {open_price:<12} {high:<12} {low:<12} {close:<12} {change:<12} {pct_chg:<12} {vol:<15} {amount:<15}")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare index_daily接口")
    result.append("  - 南华期货指数包括农产品、商品、能化、黑色、工业品、金属等各类指数")
    result.append("  - 常用指数：NHCI.NH(南华商品指数)、NHAI.NH(南华农产品指数)、CU.NH(南华沪铜指数)等")
    result.append("  - 权限要求：2000积分")
    
    return "\n".join(result)


def format_single_nh_index(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个南华期货指数的行情数据
    
    参数:
        df: 单个指数的行情数据DataFrame
        ts_code: 指数代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    # 指数名称映射
    index_names = {
        'NHAI.NH': '南华农产品指数',
        'NHCI.NH': '南华商品指数',
        'NHECI.NH': '南华能化指数',
        'NHFI.NH': '南华黑色指数',
        'NHII.NH': '南华工业品指数',
        'NHMI.NH': '南华金属指数',
        'NHNFI.NH': '南华有色金属',
        'NHPMI.NH': '南华贵金属指数',
        'CU.NH': '南华沪铜指数',
        'AU.NH': '南华沪黄金指数',
        'AG.NH': '南华沪银指数',
        'AL.NH': '南华沪铝指数',
        'ZN.NH': '南华沪锌指数',
        'PB.NH': '南华沪铅指数',
        'NI.NH': '南华沪镍指数',
        'SN.NH': '南华沪锡指数',
    }
    
    index_name = index_names.get(ts_code, ts_code)
    
    result = []
    result.append(f"📈 {ts_code} {index_name} 行情数据")
    result.append("=" * 120)
    result.append("")
    
    # 显示最近的数据（最多30条）
    display_count = min(30, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'开盘':<12} {'最高':<12} {'最低':<12} {'收盘':<12} {'涨跌点':<12} {'涨跌幅':<12} {'成交量(手)':<15} {'成交额(千元)':<15}")
    result.append("-" * 140)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(str(row['trade_date']))
        open_price = f"{row['open']:.3f}" if 'open' in row and pd.notna(row['open']) else "-"
        high = f"{row['high']:.3f}" if 'high' in row and pd.notna(row['high']) else "-"
        low = f"{row['low']:.3f}" if 'low' in row and pd.notna(row['low']) else "-"
        close = f"{row['close']:.3f}" if 'close' in row and pd.notna(row['close']) else "-"
        change = f"{row['change']:+.3f}" if 'change' in row and pd.notna(row['change']) else "-"
        pct_chg = f"{row['pct_chg']:+.2f}%" if 'pct_chg' in row and pd.notna(row['pct_chg']) else "-"
        vol = f"{row['vol']:.0f}" if 'vol' in row and pd.notna(row['vol']) else "-"
        amount = f"{row['amount']:.2f}" if 'amount' in row and pd.notna(row['amount']) else "-"
        
        result.append(f"{trade_date:<12} {open_price:<12} {high:<12} {low:<12} {close:<12} {change:<12} {pct_chg:<12} {vol:<15} {amount:<15}")
    
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
        result.append(f"指数代码: {ts_code}")
        result.append(f"指数名称: {index_name}")
        result.append(f"开盘: {latest.get('open', 0):.3f}" if pd.notna(latest.get('open')) else "开盘: -")
        result.append(f"最高: {latest.get('high', 0):.3f}" if pd.notna(latest.get('high')) else "最高: -")
        result.append(f"最低: {latest.get('low', 0):.3f}" if pd.notna(latest.get('low')) else "最低: -")
        result.append(f"收盘: {latest.get('close', 0):.3f}" if pd.notna(latest.get('close')) else "收盘: -")
        if pd.notna(latest.get('pre_close')):
            result.append(f"昨收: {latest.get('pre_close', 0):.3f}")
        if pd.notna(latest.get('change')):
            result.append(f"涨跌点: {latest.get('change', 0):+.3f}")
        if pd.notna(latest.get('pct_chg')):
            result.append(f"涨跌幅: {latest.get('pct_chg', 0):+.2f}%")
        if pd.notna(latest.get('vol')):
            result.append(f"成交量: {latest.get('vol', 0):.0f} 手")
        if pd.notna(latest.get('amount')):
            result.append(f"成交额: {latest.get('amount', 0):.2f} 千元")
    
    return "\n".join(result)


def format_fut_holding_data(df: pd.DataFrame, trade_date: str = "", symbol: str = "", exchange: str = "") -> str:
    """
    格式化期货持仓排名数据输出
    
    参数:
        df: 期货持仓排名数据DataFrame
        trade_date: 交易日期（用于显示）
        symbol: 合约代码（用于显示）
        exchange: 交易所代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的期货持仓排名数据"
    
    # 按交易日期排序（最新的在前）
    if 'trade_date' in df.columns:
        df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 交易所名称映射
    exchange_names = {
        'CFFEX': '中金所',
        'DCE': '大商所',
        'CZCE': '郑商所',
        'SHFE': '上期所',
        'INE': '上海国际能源交易中心',
        'GFEX': '广州期货交易所'
    }
    
    # 如果有多个交易日期，按日期分组显示
    if 'trade_date' in df.columns and len(df['trade_date'].unique()) > 1:
        dates = sorted(df['trade_date'].unique(), reverse=True)
        result.append("📊 期货持仓排名数据")
        result.append("=" * 180)
        result.append("")
        
        for date in dates[:10]:  # 最多显示最近10个交易日
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                # 获取交易所信息
                exchange_info = ""
                if 'exchange' in date_df.columns and not date_df['exchange'].isna().all():
                    exchanges = date_df['exchange'].dropna().unique()
                    if len(exchanges) == 1:
                        ex = exchanges[0]
                        exchange_info = f" ({exchange_names.get(ex, ex)})"
                
                result.append(f"📅 交易日期: {format_date(date)}{exchange_info}")
                result.append("-" * 180)
                
                # 按合约代码分组
                if 'symbol' in date_df.columns:
                    symbols = sorted(date_df['symbol'].dropna().unique())
                    for sym in symbols:
                        sym_df = date_df[date_df['symbol'] == sym]
                        if not sym_df.empty:
                            result.append(f"📦 合约代码: {sym} ({len(sym_df)} 条记录)")
                            result.append("")
                            result.append(f"{'期货公司':<20} {'成交量':<15} {'成交量变化':<15} {'持买仓量':<15} {'持买仓量变化':<15} {'持卖仓量':<15} {'持卖仓量变化':<15} {'交易所':<10}")
                            result.append("-" * 180)
                            
                            # 按成交量排序（降序）
                            if 'vol' in sym_df.columns:
                                sym_df = sym_df.sort_values('vol', ascending=False, na_position='last')
                            
                            for _, row in sym_df.head(30).iterrows():  # 每个合约最多显示30条
                                broker = str(row.get('broker', '-'))[:18]
                                vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
                                vol_chg = f"{row.get('vol_chg', 0):+,.0f}" if pd.notna(row.get('vol_chg')) else "-"
                                long_hld = f"{row.get('long_hld', 0):,.0f}" if pd.notna(row.get('long_hld')) else "-"
                                long_chg = f"{row.get('long_chg', 0):+,.0f}" if pd.notna(row.get('long_chg')) else "-"
                                short_hld = f"{row.get('short_hld', 0):,.0f}" if pd.notna(row.get('short_hld')) else "-"
                                short_chg = f"{row.get('short_chg', 0):+,.0f}" if pd.notna(row.get('short_chg')) else "-"
                                ex = str(row.get('exchange', '-'))[:8]
                                
                                result.append(f"{broker:<20} {vol:<15} {vol_chg:<15} {long_hld:<15} {long_chg:<15} {short_hld:<15} {short_chg:<15} {ex:<10}")
                            
                            if len(sym_df) > 30:
                                result.append(f"  ... 还有 {len(sym_df) - 30} 条记录未显示")
                            result.append("")
                else:
                    # 如果没有symbol字段，直接显示所有记录
                    result.append(f"{'期货公司':<20} {'成交量':<15} {'成交量变化':<15} {'持买仓量':<15} {'持买仓量变化':<15} {'持卖仓量':<15} {'持卖仓量变化':<15} {'交易所':<10}")
                    result.append("-" * 180)
                    
                    # 按成交量排序（降序）
                    if 'vol' in date_df.columns:
                        date_df = date_df.sort_values('vol', ascending=False, na_position='last')
                    
                    for _, row in date_df.head(50).iterrows():
                        broker = str(row.get('broker', '-'))[:18]
                        vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
                        vol_chg = f"{row.get('vol_chg', 0):+,.0f}" if pd.notna(row.get('vol_chg')) else "-"
                        long_hld = f"{row.get('long_hld', 0):,.0f}" if pd.notna(row.get('long_hld')) else "-"
                        long_chg = f"{row.get('long_chg', 0):+,.0f}" if pd.notna(row.get('long_chg')) else "-"
                        short_hld = f"{row.get('short_hld', 0):,.0f}" if pd.notna(row.get('short_hld')) else "-"
                        short_chg = f"{row.get('short_chg', 0):+,.0f}" if pd.notna(row.get('short_chg')) else "-"
                        ex = str(row.get('exchange', '-'))[:8]
                        
                        result.append(f"{broker:<20} {vol:<15} {vol_chg:<15} {long_hld:<15} {long_chg:<15} {short_hld:<15} {short_chg:<15} {ex:<10}")
                    
                    if len(date_df) > 50:
                        result.append(f"  ... 还有 {len(date_df) - 50} 条记录未显示")
                result.append("")
    else:
        # 单个日期或单个合约，使用详细格式
        result.append("📊 期货持仓排名数据")
        result.append("=" * 180)
        result.append("")
        
        # 显示查询条件
        if trade_date:
            result.append(f"📅 交易日期: {format_date(trade_date)}")
        if symbol:
            result.append(f"📦 合约代码: {symbol}")
        if exchange:
            exchange_name = exchange_names.get(exchange.upper(), exchange)
            result.append(f"🏢 交易所: {exchange_name}({exchange.upper()})")
        result.append("")
        
        # 按合约代码分组
        if 'symbol' in df.columns and len(df['symbol'].dropna().unique()) > 1:
            symbols = sorted(df['symbol'].dropna().unique())
            for sym in symbols:
                sym_df = df[df['symbol'] == sym]
                if not sym_df.empty:
                    result.append(f"📦 合约代码: {sym} ({len(sym_df)} 条记录)")
                    result.append("")
                    result.append(f"{'期货公司':<20} {'成交量':<15} {'成交量变化':<15} {'持买仓量':<15} {'持买仓量变化':<15} {'持卖仓量':<15} {'持卖仓量变化':<15} {'交易所':<10}")
                    result.append("-" * 180)
                    
                    # 按成交量排序（降序）
                    if 'vol' in sym_df.columns:
                        sym_df = sym_df.sort_values('vol', ascending=False, na_position='last')
                    
                    for _, row in sym_df.head(50).iterrows():
                        broker = str(row.get('broker', '-'))[:18]
                        vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
                        vol_chg = f"{row.get('vol_chg', 0):+,.0f}" if pd.notna(row.get('vol_chg')) else "-"
                        long_hld = f"{row.get('long_hld', 0):,.0f}" if pd.notna(row.get('long_hld')) else "-"
                        long_chg = f"{row.get('long_chg', 0):+,.0f}" if pd.notna(row.get('long_chg')) else "-"
                        short_hld = f"{row.get('short_hld', 0):,.0f}" if pd.notna(row.get('short_hld')) else "-"
                        short_chg = f"{row.get('short_chg', 0):+,.0f}" if pd.notna(row.get('short_chg')) else "-"
                        ex = str(row.get('exchange', '-'))[:8]
                        
                        result.append(f"{broker:<20} {vol:<15} {vol_chg:<15} {long_hld:<15} {long_chg:<15} {short_hld:<15} {short_chg:<15} {ex:<10}")
                    
                    if len(sym_df) > 50:
                        result.append(f"  ... 还有 {len(sym_df) - 50} 条记录未显示")
                    result.append("")
        else:
            # 单个合约或没有symbol字段，直接显示所有记录
            result.append(f"共找到 {len(df)} 条记录")
            result.append("")
            result.append(f"{'期货公司':<20} {'成交量':<15} {'成交量变化':<15} {'持买仓量':<15} {'持买仓量变化':<15} {'持卖仓量':<15} {'持卖仓量变化':<15} {'交易所':<10}")
            result.append("-" * 180)
            
            # 按成交量排序（降序）
            if 'vol' in df.columns:
                df = df.sort_values('vol', ascending=False, na_position='last')
            
            display_count = min(100, len(df))
            for _, row in df.head(display_count).iterrows():
                broker = str(row.get('broker', '-'))[:18]
                vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
                vol_chg = f"{row.get('vol_chg', 0):+,.0f}" if pd.notna(row.get('vol_chg')) else "-"
                long_hld = f"{row.get('long_hld', 0):,.0f}" if pd.notna(row.get('long_hld')) else "-"
                long_chg = f"{row.get('long_chg', 0):+,.0f}" if pd.notna(row.get('long_chg')) else "-"
                short_hld = f"{row.get('short_hld', 0):,.0f}" if pd.notna(row.get('short_hld')) else "-"
                short_chg = f"{row.get('short_chg', 0):+,.0f}" if pd.notna(row.get('short_chg')) else "-"
                ex = str(row.get('exchange', '-'))[:8]
                
                result.append(f"{broker:<20} {vol:<15} {vol_chg:<15} {long_hld:<15} {long_chg:<15} {short_hld:<15} {short_chg:<15} {ex:<10}")
            
            if len(df) > display_count:
                result.append("")
                result.append(f"（共 {len(df)} 条数据，仅显示前 {display_count} 条）")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare fut_holding接口")
    result.append("  - 显示期货公司会员的成交量、持仓量等数据")
    result.append("  - 成交量变化、持仓量变化：正数表示增加，负数表示减少")
    result.append("  - 权限要求：2000积分")
    result.append("  - 限量：单次最大可调取5000条数据")
    
    return "\n".join(result)


def format_fut_wsr_data(df: pd.DataFrame, trade_date: str = "", symbol: str = "", exchange: str = "") -> str:
    """
    格式化期货仓单日报数据输出
    
    参数:
        df: 期货仓单日报数据DataFrame
        trade_date: 交易日期（用于显示）
        symbol: 产品代码（用于显示）
        exchange: 交易所代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的期货仓单日报数据"
    
    result = []
    result.append("📦 期货仓单日报数据")
    result.append("=" * 180)
    result.append("")
    
    # 按交易日期排序（最新的在前）
    if 'trade_date' in df.columns:
        df = df.sort_values('trade_date', ascending=False)
    
    # 如果有多个产品代码，按产品代码分组显示
    if 'symbol' in df.columns and len(df['symbol'].unique()) > 1:
        symbols = sorted(df['symbol'].unique())
        result.append(f"共找到 {len(df)} 条记录，涉及 {len(symbols)} 个产品")
        result.append("")
        
        # 按产品代码分组显示
        for sym in symbols:
            sym_df = df[df['symbol'] == sym]
            if not sym_df.empty:
                fut_name = sym_df.iloc[0].get('fut_name', sym)
                result.append(f"📊 {sym} ({fut_name}) 仓单数据")
                result.append("-" * 180)
                
                # 如果有多个日期，按日期分组
                if 'trade_date' in sym_df.columns and len(sym_df['trade_date'].unique()) > 1:
                    dates = sorted(sym_df['trade_date'].unique(), reverse=True)
                    for date in dates[:5]:  # 最多显示最近5个交易日
                        date_df = sym_df[sym_df['trade_date'] == date]
                        if not date_df.empty:
                            result.append(f"📅 交易日期: {format_date(str(date))}")
                            result.append(f"{'仓库名称':<25} {'昨日仓单量':<15} {'今日仓单量':<15} {'增减量':<15} {'单位':<10}")
                            result.append("-" * 180)
                            
                            # 按仓单量排序（降序）
                            if 'vol' in date_df.columns:
                                date_df = date_df.sort_values('vol', ascending=False, na_position='last')
                            
                            # 只显示有仓单的仓库
                            date_df_with_vol = date_df[date_df['vol'] > 0]
                            if date_df_with_vol.empty:
                                date_df_with_vol = date_df.head(10)
                            
                            for _, row in date_df_with_vol.head(50).iterrows():
                                warehouse = str(row.get('warehouse', '-'))[:23]
                                pre_vol = f"{row.get('pre_vol', 0):,.0f}" if pd.notna(row.get('pre_vol')) else "-"
                                vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
                                vol_chg = f"{row.get('vol_chg', 0):+,.0f}" if pd.notna(row.get('vol_chg')) else "-"
                                unit = str(row.get('unit', '-'))[:8]
                                
                                result.append(f"{warehouse:<25} {pre_vol:<15} {vol:<15} {vol_chg:<15} {unit:<10}")
                            
                            if len(date_df) > 50:
                                result.append(f"  ... 还有 {len(date_df) - 50} 条记录未显示")
                            result.append("")
                else:
                    # 单个日期，直接显示所有仓库
                    result.append(f"{'仓库名称':<25} {'昨日仓单量':<15} {'今日仓单量':<15} {'增减量':<15} {'单位':<10}")
                    result.append("-" * 180)
                    
                    # 按仓单量排序（降序）
                    if 'vol' in sym_df.columns:
                        sym_df = sym_df.sort_values('vol', ascending=False, na_position='last')
                    
                    # 只显示有仓单的仓库
                    sym_df_with_vol = sym_df[sym_df['vol'] > 0]
                    if sym_df_with_vol.empty:
                        sym_df_with_vol = sym_df.head(10)
                    
                    for _, row in sym_df_with_vol.head(50).iterrows():
                        warehouse = str(row.get('warehouse', '-'))[:23]
                        pre_vol = f"{row.get('pre_vol', 0):,.0f}" if pd.notna(row.get('pre_vol')) else "-"
                        vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
                        vol_chg = f"{row.get('vol_chg', 0):+,.0f}" if pd.notna(row.get('vol_chg')) else "-"
                        unit = str(row.get('unit', '-'))[:8]
                        
                        result.append(f"{warehouse:<25} {pre_vol:<15} {vol:<15} {vol_chg:<15} {unit:<10}")
                    
                    if len(sym_df) > 50:
                        result.append(f"  ... 还有 {len(sym_df) - 50} 条记录未显示")
                    result.append("")
        
        if len(symbols) > 10:
            result.append(f"  ... 还有 {len(symbols) - 10} 个产品未显示")
    else:
        # 单个产品或没有symbol字段，直接显示所有记录
        if 'symbol' in df.columns and not df.empty:
            sym = df.iloc[0].get('symbol', '-')
            fut_name = df.iloc[0].get('fut_name', '-')
            result.append(f"📊 {sym} ({fut_name}) 仓单数据")
        else:
            result.append(f"共找到 {len(df)} 条记录")
        result.append("")
        
        # 如果有多个日期，按日期分组
        if 'trade_date' in df.columns and len(df['trade_date'].unique()) > 1:
            dates = sorted(df['trade_date'].unique(), reverse=True)
            for date in dates[:10]:  # 最多显示最近10个交易日
                date_df = df[df['trade_date'] == date]
                if not date_df.empty:
                    result.append(f"📅 交易日期: {format_date(str(date))}")
                    result.append(f"{'仓库名称':<25} {'昨日仓单量':<15} {'今日仓单量':<15} {'增减量':<15} {'单位':<10}")
                    result.append("-" * 180)
                    
                    # 按仓单量排序（降序）
                    if 'vol' in date_df.columns:
                        date_df = date_df.sort_values('vol', ascending=False, na_position='last')
                    
                    # 只显示有仓单的仓库
                    date_df_with_vol = date_df[date_df['vol'] > 0]
                    if date_df_with_vol.empty:
                        date_df_with_vol = date_df.head(10)
                    
                    display_count = min(100, len(date_df_with_vol))
                    for _, row in date_df_with_vol.head(display_count).iterrows():
                        warehouse = str(row.get('warehouse', '-'))[:23]
                        pre_vol = f"{row.get('pre_vol', 0):,.0f}" if pd.notna(row.get('pre_vol')) else "-"
                        vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
                        vol_chg = f"{row.get('vol_chg', 0):+,.0f}" if pd.notna(row.get('vol_chg')) else "-"
                        unit = str(row.get('unit', '-'))[:8]
                        
                        result.append(f"{warehouse:<25} {pre_vol:<15} {vol:<15} {vol_chg:<15} {unit:<10}")
                    
                    if len(date_df_with_vol) > display_count:
                        result.append(f"  ... 还有 {len(date_df_with_vol) - display_count} 条记录未显示")
                    result.append("")
        else:
            # 单个日期，直接显示所有仓库
            result.append(f"{'仓库名称':<25} {'昨日仓单量':<15} {'今日仓单量':<15} {'增减量':<15} {'单位':<10}")
            result.append("-" * 180)
            
            # 按仓单量排序（降序）
            if 'vol' in df.columns:
                df = df.sort_values('vol', ascending=False, na_position='last')
            
            # 只显示有仓单的仓库
            df_with_vol = df[df['vol'] > 0]
            if df_with_vol.empty:
                df_with_vol = df.head(10)
            
            display_count = min(100, len(df_with_vol))
            for _, row in df_with_vol.head(display_count).iterrows():
                warehouse = str(row.get('warehouse', '-'))[:23]
                pre_vol = f"{row.get('pre_vol', 0):,.0f}" if pd.notna(row.get('pre_vol')) else "-"
                vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
                vol_chg = f"{row.get('vol_chg', 0):+,.0f}" if pd.notna(row.get('vol_chg')) else "-"
                unit = str(row.get('unit', '-'))[:8]
                
                result.append(f"{warehouse:<25} {pre_vol:<15} {vol:<15} {vol_chg:<15} {unit:<10}")
            
            if len(df_with_vol) > display_count:
                result.append("")
                result.append(f"（共 {len(df_with_vol)} 条数据，仅显示前 {display_count} 条）")
    
    # 显示汇总信息
    if not df.empty and 'vol' in df.columns:
        total_vol = df['vol'].sum()
        total_pre_vol = df['pre_vol'].sum() if 'pre_vol' in df.columns else 0
        total_vol_chg = total_vol - total_pre_vol if total_pre_vol > 0 else 0
        
        result.append("")
        result.append("📊 仓单汇总：")
        result.append("-" * 180)
        if 'trade_date' in df.columns and len(df['trade_date'].unique()) == 1:
            result.append(f"交易日期: {format_date(str(df.iloc[0].get('trade_date', '-')))}")
        result.append(f"总仓单量: {total_vol:,.0f} {df.iloc[0].get('unit', '')}")
        if total_pre_vol > 0:
            result.append(f"前一日仓单量: {total_pre_vol:,.0f} {df.iloc[0].get('unit', '')}")
            result.append(f"仓单变化量: {total_vol_chg:+,.0f} {df.iloc[0].get('unit', '')}")
        result.append(f"仓库数量: {len(df)} 个")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare fut_wsr接口")
    result.append("  - 显示各仓库/厂库的仓单变化情况")
    result.append("  - 增减量：正数表示增加，负数表示减少")
    result.append("  - 权限要求：2000积分")
    result.append("  - 限量：单次最大可调取1000条数据")
    
    return "\n".join(result)


def format_fut_min_data(df: pd.DataFrame, ts_code: str = "", freq: str = "1MIN", date_str: str = "") -> str:
    """
    格式化期货实时分钟行情数据输出
    
    参数:
        df: 期货分钟行情数据DataFrame
        ts_code: 合约代码（用于显示）
        freq: 分钟频度（用于显示）
        date_str: 回放日期（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的期货分钟行情数据"
    
    result = []
    result.append("📈 期货实时分钟行情数据")
    result.append("=" * 180)
    result.append("")
    
    # 按时间排序（最新的在前）
    if 'time' in df.columns:
        df = df.sort_values('time', ascending=False)
    
    # 如果有多个合约，按合约代码分组显示
    if 'code' in df.columns and len(df['code'].unique()) > 1:
        codes = sorted(df['code'].unique())
        result.append(f"共找到 {len(df)} 条记录，涉及 {len(codes)} 个合约")
        result.append(f"分钟频度: {freq}")
        if date_str:
            result.append(f"回放日期: {date_str}")
        result.append("")
        
        # 按合约代码分组显示
        for code in codes:
            code_df = df[df['code'] == code]
            if not code_df.empty:
                result.append(f"📊 {code} 分钟行情数据")
                result.append("-" * 180)
                result.append(f"{'交易时间':<20} {'开盘':<12} {'最高':<12} {'最低':<12} {'收盘':<12} {'成交量':<15} {'成交金额':<15} {'持仓量':<15}")
                result.append("-" * 180)
                
                # 按时间排序（最新的在前）
                if 'time' in code_df.columns:
                    code_df = code_df.sort_values('time', ascending=False)
                
                display_count = min(100, len(code_df))
                for _, row in code_df.head(display_count).iterrows():
                    time_str = str(row.get('time', '-'))[:18]
                    open_price = f"{row.get('open', 0):.2f}" if pd.notna(row.get('open')) else "-"
                    high = f"{row.get('high', 0):.2f}" if pd.notna(row.get('high')) else "-"
                    low = f"{row.get('low', 0):.2f}" if pd.notna(row.get('low')) else "-"
                    close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
                    vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
                    amount = f"{row.get('amount', 0):,.2f}" if pd.notna(row.get('amount')) else "-"
                    oi = f"{row.get('oi', 0):,.0f}" if pd.notna(row.get('oi')) else "-"
                    
                    result.append(f"{time_str:<20} {open_price:<12} {high:<12} {low:<12} {close:<12} {vol:<15} {amount:<15} {oi:<15}")
                
                if len(code_df) > display_count:
                    result.append(f"  ... 还有 {len(code_df) - display_count} 条记录未显示")
                result.append("")
        
        if len(codes) > 10:
            result.append(f"  ... 还有 {len(codes) - 10} 个合约未显示")
    else:
        # 单个合约或没有code字段，直接显示所有记录
        if 'code' in df.columns and not df.empty:
            code = df.iloc[0].get('code', ts_code or '-')
            result.append(f"📊 {code} 分钟行情数据")
        else:
            result.append(f"共找到 {len(df)} 条记录")
        result.append("")
        result.append(f"分钟频度: {freq}")
        if date_str:
            result.append(f"回放日期: {date_str}")
        result.append("")
        result.append(f"{'交易时间':<20} {'开盘':<12} {'最高':<12} {'最低':<12} {'收盘':<12} {'成交量':<15} {'成交金额':<15} {'持仓量':<15}")
        result.append("-" * 180)
        
        display_count = min(200, len(df))
        for _, row in df.head(display_count).iterrows():
            time_str = str(row.get('time', '-'))[:18]
            open_price = f"{row.get('open', 0):.2f}" if pd.notna(row.get('open')) else "-"
            high = f"{row.get('high', 0):.2f}" if pd.notna(row.get('high')) else "-"
            low = f"{row.get('low', 0):.2f}" if pd.notna(row.get('low')) else "-"
            close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
            vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
            amount = f"{row.get('amount', 0):,.2f}" if pd.notna(row.get('amount')) else "-"
            oi = f"{row.get('oi', 0):,.0f}" if pd.notna(row.get('oi')) else "-"
            
            result.append(f"{time_str:<20} {open_price:<12} {high:<12} {low:<12} {close:<12} {vol:<15} {amount:<15} {oi:<15}")
        
        if len(df) > display_count:
            result.append("")
            result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 180)
        if 'code' in latest:
            result.append(f"合约代码: {latest.get('code', '-')}")
        if 'freq' in latest:
            result.append(f"分钟频度: {latest.get('freq', '-')}")
        if 'time' in latest:
            result.append(f"交易时间: {latest.get('time', '-')}")
        if 'open' in latest and pd.notna(latest.get('open')):
            result.append(f"开盘: {latest.get('open', 0):.2f}")
        if 'high' in latest and pd.notna(latest.get('high')):
            result.append(f"最高: {latest.get('high', 0):.2f}")
        if 'low' in latest and pd.notna(latest.get('low')):
            result.append(f"最低: {latest.get('low', 0):.2f}")
        if 'close' in latest and pd.notna(latest.get('close')):
            result.append(f"收盘: {latest.get('close', 0):.2f}")
        if 'vol' in latest and pd.notna(latest.get('vol')):
            result.append(f"成交量: {latest.get('vol', 0):,.0f}")
        if 'amount' in latest and pd.notna(latest.get('amount')):
            result.append(f"成交金额: {latest.get('amount', 0):,.2f}")
        if 'oi' in latest and pd.notna(latest.get('oi')):
            result.append(f"持仓量: {latest.get('oi', 0):,.0f}")
    
    result.append("")
    result.append("📝 说明：")
    if date_str:
        result.append("  - 数据来源：Tushare rt_fut_min_daily接口（历史回放）")
        result.append("  - 提供当日开市以来所有历史分钟数据")
    else:
        result.append("  - 数据来源：Tushare rt_fut_min接口（实时）")
        result.append("  - 获取全市场期货合约实时分钟数据")
    result.append("  - 支持1min/5min/15min/30min/60min行情")
    result.append("  - 权限要求：需单独开权限，正式权限请参阅权限说明")
    result.append("  - 限量：每分钟可以请求500次，支持多个合约同时提取")
    result.append("  - 注意：如果需要主力合约分钟，请先通过主力mapping接口获取对应的合约代码")
    
    return "\n".join(result)


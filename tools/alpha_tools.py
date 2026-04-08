"""Alpha策略分析相关MCP工具"""
import pandas as pd
import tushare as ts
from typing import TYPE_CHECKING, List
from config.token_manager import get_tushare_token
from cache.cache_manager import cache_manager
from tools.alpha_strategy_analyzer import (
    analyze_sector_alpha,
    rank_sectors_alpha,
    format_alpha_analysis,
    calculate_alpha_rank_velocity
)

def get_l1_sector_codes() -> List[str]:
    """获取申万一级行业代码列表"""
    return [
        "801010.SI",  # 农林牧渔
        "801030.SI",  # 基础化工
        "801040.SI",  # 钢铁
        "801050.SI",  # 有色金属
        "801080.SI",  # 电子
        "801110.SI",  # 家用电器
        "801120.SI",  # 食品饮料
        "801130.SI",  # 纺织服饰
        "801140.SI",  # 轻工制造
        "801150.SI",  # 医药生物
        "801160.SI",  # 公用事业
        "801170.SI",  # 交通运输
        "801180.SI",  # 房地产
        "801200.SI",  # 商贸零售
        "801210.SI",  # 社会服务
        "801230.SI",  # 综合
        "801710.SI",  # 建筑材料
        "801720.SI",  # 建筑装饰
        "801730.SI",  # 电力设备
        "801740.SI",  # 国防军工
        "801750.SI",  # 计算机
        "801760.SI",  # 传媒
        "801770.SI",  # 通信
        "801780.SI",  # 银行
        "801790.SI",  # 非银金融
        "801880.SI",  # 汽车
        "801890.SI",  # 机械设备
        "801950.SI",  # 煤炭
        "801960.SI",  # 石油石化
        "801970.SI",  # 环保
        "801980.SI",  # 美容护理
    ]

def get_l1_sector_name_map() -> dict:
    """获取申万一级行业代码到名称的映射"""
    return {
        "801010.SI": "农林牧渔",
        "801030.SI": "基础化工",
        "801040.SI": "钢铁",
        "801050.SI": "有色金属",
        "801080.SI": "电子",
        "801110.SI": "家用电器",
        "801120.SI": "食品饮料",
        "801130.SI": "纺织服饰",
        "801140.SI": "轻工制造",
        "801150.SI": "医药生物",
        "801160.SI": "公用事业",
        "801170.SI": "交通运输",
        "801180.SI": "房地产",
        "801200.SI": "商贸零售",
        "801210.SI": "社会服务",
        "801230.SI": "综合",
        "801710.SI": "建筑材料",
        "801720.SI": "建筑装饰",
        "801730.SI": "电力设备",
        "801740.SI": "国防军工",
        "801750.SI": "计算机",
        "801760.SI": "传媒",
        "801770.SI": "通信",
        "801780.SI": "银行",
        "801790.SI": "非银金融",
        "801880.SI": "汽车",
        "801890.SI": "机械设备",
        "801950.SI": "煤炭",
        "801960.SI": "石油石化",
        "801970.SI": "环保",
        "801980.SI": "美容护理",
    }

def get_l2_sector_name_map() -> dict:
    """获取申万二级行业代码到名称的映射（仅包含已发布指数的行业）"""
    return {
        "801012.SI": "农产品加工",
        "801014.SI": "饲料",
        "801015.SI": "渔业",
        "801016.SI": "种植业",
        "801017.SI": "养殖业",
        "801018.SI": "动物保健Ⅱ",
        "801032.SI": "化学纤维",
        "801033.SI": "化学原料",
        "801034.SI": "化学制品",
        "801036.SI": "塑料",
        "801037.SI": "橡胶",
        "801038.SI": "农化制品",
        "801039.SI": "非金属材料Ⅱ",
        "801043.SI": "冶钢原料",
        "801044.SI": "普钢",
        "801045.SI": "特钢Ⅱ",
        "801051.SI": "金属新材料",
        "801053.SI": "贵金属",
        "801054.SI": "小金属",
        "801055.SI": "工业金属",
        "801056.SI": "能源金属",
        "801072.SI": "通用设备",
        "801074.SI": "专用设备",
        "801076.SI": "轨交设备Ⅱ",
        "801077.SI": "工程机械",
        "801078.SI": "自动化设备",
        "801081.SI": "半导体",
        "801082.SI": "其他电子Ⅱ",
        "801083.SI": "元件",
        "801084.SI": "光学光电子",
        "801085.SI": "消费电子",
        "801086.SI": "电子化学品Ⅱ",
        "801092.SI": "汽车服务",
        "801093.SI": "汽车零部件",
        "801095.SI": "乘用车",
        "801096.SI": "商用车",
        "801101.SI": "计算机设备",
        "801102.SI": "通信设备",
        "801103.SI": "IT服务Ⅱ",
        "801104.SI": "软件开发",
        "801111.SI": "白色家电",
        "801112.SI": "黑色家电",
        "801113.SI": "小家电",
        "801114.SI": "厨卫电器",
        "801115.SI": "照明设备Ⅱ",
        "801116.SI": "家电零部件Ⅱ",
        "801124.SI": "食品加工",
        "801125.SI": "白酒Ⅱ",
        "801126.SI": "非白酒",
        "801127.SI": "饮料乳品",
        "801128.SI": "休闲食品",
        "801129.SI": "调味发酵品Ⅱ",
        "801131.SI": "纺织制造",
        "801132.SI": "服装家纺",
        "801133.SI": "饰品",
        "801141.SI": "包装印刷",
        "801142.SI": "家居用品",
        "801143.SI": "造纸",
        "801145.SI": "文娱用品",
        "801151.SI": "化学制药",
        "801152.SI": "生物制品",
        "801153.SI": "医疗器械",
        "801154.SI": "医药商业",
        "801155.SI": "中药Ⅱ",
        "801156.SI": "医疗服务",
        "801161.SI": "电力",
        "801163.SI": "燃气Ⅱ",
        "801178.SI": "物流",
        "801179.SI": "铁路公路",
        "801181.SI": "房地产开发",
        "801183.SI": "房地产服务",
        "801191.SI": "多元金融",
        "801193.SI": "证券Ⅱ",
        "801194.SI": "保险Ⅱ",
        "801202.SI": "贸易Ⅱ",
        "801203.SI": "一般零售",
        "801204.SI": "专业连锁Ⅱ",
        "801206.SI": "互联网电商",
        "801218.SI": "专业服务",
        "801219.SI": "酒店餐饮",
        "801223.SI": "通信服务",
        "801231.SI": "综合Ⅱ",
        "801711.SI": "水泥",
        "801712.SI": "玻璃玻纤",
        "801713.SI": "装修建材",
        "801721.SI": "房屋建设Ⅱ",
        "801722.SI": "装修装饰Ⅱ",
        "801723.SI": "基础建设",
        "801724.SI": "专业工程",
        "801726.SI": "工程咨询服务Ⅱ",
        "801731.SI": "电机Ⅱ",
        "801733.SI": "其他电源设备Ⅱ",
        "801735.SI": "光伏设备",
        "801736.SI": "风电设备",
        "801737.SI": "电池",
        "801738.SI": "电网设备",
        "801741.SI": "航天装备Ⅱ",
        "801742.SI": "航空装备Ⅱ",
        "801743.SI": "地面兵装Ⅱ",
        "801744.SI": "航海装备Ⅱ",
        "801745.SI": "军工电子Ⅱ",
        "801764.SI": "游戏Ⅱ",
        "801765.SI": "广告营销",
        "801766.SI": "影视院线",
        "801767.SI": "数字媒体",
        "801769.SI": "出版",
        "801782.SI": "国有大型银行Ⅱ",
        "801783.SI": "股份制银行Ⅱ",
        "801784.SI": "城商行Ⅱ",
        "801785.SI": "农商行Ⅱ",
        "801881.SI": "摩托车及其他",
        "801951.SI": "煤炭开采",
        "801952.SI": "焦炭Ⅱ",
        "801962.SI": "油服工程",
        "801963.SI": "炼化及贸易",
        "801971.SI": "环境治理",
        "801972.SI": "环保设备Ⅱ",
        "801981.SI": "个护用品",
        "801982.SI": "化妆品",
        "801991.SI": "航空机场",
        "801992.SI": "航运港口",
        "801993.SI": "旅游及景区",
        "801994.SI": "教育",
        "801995.SI": "电视广播Ⅱ"
    }

def get_l2_sector_codes() -> List[str]:
    """获取申万二级行业代码列表（仅包含已发布指数的行业）"""
    return list(get_l2_sector_name_map().keys())

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

def register_alpha_tools(mcp: "FastMCP"):
    """注册Alpha策略分析工具"""
    
    @mcp.tool()
    def analyze_sector_alpha_strategy(
        sector_code: str = "",
        benchmark_code: str = "000001.SH",
        end_date: str = ""
    ) -> str:
        """
        分析单个板块的相对强度Alpha
        
        参数:
            sector_code: 板块指数代码（如：801010.SI农林牧渔、801080.SI电子等）
            benchmark_code: 基准指数代码（默认：000001.SH上证指数）
            end_date: 结束日期（YYYYMMDD格式，如：20241124，默认今天）
        
        返回:
            包含Alpha分析结果的格式化字符串
        
        说明:
            - 计算2天和5天的区间收益率
            - 计算超额收益Alpha = 板块收益 - 基准收益
            - 综合得分 = Alpha_2 × 60% + Alpha_5 × 40%
        """
        if not sector_code:
            return "请提供板块指数代码(如：801010.SI、801080.SI等)"
        
        # 如果end_date为空，使用None让analyze_sector_alpha使用默认值
        if end_date == "":
            end_date = None
        
        result = analyze_sector_alpha(sector_code, benchmark_code, end_date)
        
        if "error" in result:
            return result["error"]
        
        # 格式化输出
        output = []
        output.append(f"📊 {sector_code} 相对强度Alpha分析")
        output.append("=" * 80)
        output.append("")
        output.append(f"基准指数: {result['benchmark_code']}")
        output.append(f"请求日期: {result['end_date']}")
        
        # 检查实际数据日期
        actual_date = result.get('actual_date')
        if actual_date:
            output.append(f"实际数据日期: {actual_date}")
            if result['end_date'] and str(result['end_date']) != str(actual_date):
                 output.append("⚠️ 注意：实际数据日期与请求日期不一致，可能是当天数据尚未更新")
        
        output.append("")
        output.append("📈 收益率分析：")
        output.append("-" * 80)
        
        if pd.notna(result.get('r_sector_1')):
            output.append(f"板块当天收益率: {result['r_sector_1']*100:.2f}%")
        else:
            output.append("板块当天收益率: 数据不足")
            
        if pd.notna(result['r_sector_2']):
            output.append(f"板块2日收益率: {result['r_sector_2']*100:.2f}%")
        else:
            output.append("板块2日收益率: 数据不足")
        
        if pd.notna(result['r_sector_5']):
            output.append(f"板块5日收益率: {result['r_sector_5']*100:.2f}%")
        else:
            output.append("板块5日收益率: 数据不足")
            
        if pd.notna(result.get('r_benchmark_1')):
            output.append(f"基准当天收益率: {result['r_benchmark_1']*100:.2f}%")
        else:
            output.append("基准当天收益率: 数据不足")
        
        if pd.notna(result['r_benchmark_2']):
            output.append(f"基准2日收益率: {result['r_benchmark_2']*100:.2f}%")
        else:
            output.append("基准2日收益率: 数据不足")
        
        if pd.notna(result['r_benchmark_5']):
            output.append(f"基准5日收益率: {result['r_benchmark_5']*100:.2f}%")
        else:
            output.append("基准5日收益率: 数据不足")
        
        output.append("")
        output.append("🎯 Alpha分析：")
        output.append("-" * 80)
        
        if pd.notna(result.get('alpha_1')):
            alpha_1_pct = result['alpha_1'] * 100
            status_1 = "✅ 跑赢大盘" if alpha_1_pct > 0 else "❌ 跑输大盘"
            output.append(f"当天Alpha: {alpha_1_pct:+.2f}% {status_1}")
        else:
            output.append("当天Alpha: 数据不足")
            
        if pd.notna(result['alpha_2']):
            alpha_2_pct = result['alpha_2'] * 100
            status_2 = "✅ 跑赢大盘" if alpha_2_pct > 0 else "❌ 跑输大盘"
            output.append(f"2日Alpha: {alpha_2_pct:+.2f}% {status_2}")
        else:
            output.append("2日Alpha: 数据不足")
        
        if pd.notna(result['alpha_5']):
            alpha_5_pct = result['alpha_5'] * 100
            status_5 = "✅ 跑赢大盘" if alpha_5_pct > 0 else "❌ 跑输大盘"
            output.append(f"5日Alpha: {alpha_5_pct:+.2f}% {status_5}")
        else:
            output.append("5日Alpha: 数据不足")
        
        output.append("")
        output.append("🏆 综合评分：")
        output.append("-" * 80)
        
        if pd.notna(result['score']):
            score_pct = result['score'] * 100
            if score_pct > 5:
                rating = "⭐⭐⭐ 非常强势"
            elif score_pct > 2:
                rating = "⭐⭐ 强势"
            elif score_pct > 0:
                rating = "⭐ 略强"
            elif score_pct > -2:
                rating = "➖ 中性"
            elif score_pct > -5:
                rating = "⚠️ 弱势"
            else:
                rating = "❌ 非常弱势"
            
            output.append(f"综合得分: {score_pct:+.2f}% {rating}")
            output.append("")
            output.append("计算公式: 得分 = Alpha_2 × 60% + Alpha_5 × 40%")
        else:
            output.append("综合得分: 数据不足")
        
        return "\n".join(output)
    
    @mcp.tool()
    def rank_sectors_by_alpha(
        benchmark_code: str = "000001.SH",
        end_date: str = "",
        top_n: int = 10
    ) -> str:
        """
        对所有申万一级行业进行Alpha排名
        
        参数:
            benchmark_code: 基准指数代码（默认：000001.SH上证指数）
            end_date: 结束日期（YYYYMMDD格式，默认今天）
            top_n: 显示前N名（默认10）
        
        返回:
            包含排名结果的格式化字符串
        
        说明:
            - 自动分析所有31个申万一级行业
            - 按综合得分降序排列
            - 显示前N名强势板块
        """
        # 如果end_date为空，使用None让analyze_sector_alpha使用默认值
        if end_date == "":
            end_date = None
        
        # 申万一级行业代码列表
        sector_codes = get_l1_sector_codes()
        
        df = rank_sectors_alpha(sector_codes, benchmark_code, end_date)
        
        if df.empty:
            return "无法获取板块数据，请检查网络连接和token配置。\n提示：如果所有板块都返回错误，可能是数据获取失败，请检查Tushare token是否有效。"
        
        # 添加行业名称
        name_map = get_l1_sector_name_map()
        df['name'] = df['sector_code'].map(name_map).fillna(df['sector_code'])
        
        # 显示所有排名（如果top_n大于等于总数，显示全部）
        if top_n >= len(df):
            df_display = df
        else:
            df_display = df.head(top_n)
        
        result = format_alpha_analysis(df_display)
        
        # 如果只显示了部分，添加提示
        if top_n < len(df):
            result += f"\n\n（共 {len(df)} 个行业，仅显示前 {top_n} 名）"
        
        return result
    
    @mcp.tool()
    def rank_l2_sectors_by_alpha(
        benchmark_code: str = "000001.SH",
        end_date: str = "",
        top_n: int = 20
    ) -> str:
        """
        对所有申万二级行业进行Alpha排名
        
        参数:
            benchmark_code: 基准指数代码（默认：000001.SH上证指数）
            end_date: 结束日期（YYYYMMDD格式，默认今天）
            top_n: 显示前N名（默认20）
        
        返回:
            包含排名结果的格式化字符串
        
        说明:
            - 自动分析所有已发布指数的申万二级行业
            - 按综合得分降序排列
            - 显示前N名强势板块
        """
        # 如果end_date为空，使用None让analyze_sector_alpha使用默认值
        if end_date == "":
            end_date = None
        
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 申万二级行业代码列表
            sector_codes = get_l2_sector_codes()
            
            # 进行Alpha排名
            df = rank_sectors_alpha(sector_codes, benchmark_code, end_date)
            
            if df.empty:
                return "无法获取板块数据，请检查网络连接和token配置。\n提示：如果所有板块都返回错误，可能是数据获取失败，请检查Tushare token是否有效。"
            
            # 添加行业名称
            name_map = get_l2_sector_name_map()
            df['name'] = df['sector_code'].map(name_map).fillna(df['sector_code'])
            
            # 显示所有排名（如果top_n大于等于总数，显示全部）
            if top_n >= len(df):
                df_display = df
            else:
                df_display = df.head(top_n)
            
            result = format_alpha_analysis(df_display)
            
            # 如果只显示了部分，添加提示
            if top_n < len(df):
                result += f"\n\n（共 {len(df)} 个二级行业，仅显示前 {top_n} 名）"
            else:
                result += f"\n\n（共 {len(df)} 个二级行业）"
            
            return result
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def rank_l2_sectors_alpha_velocity(
        benchmark_code: str = "000001.SH",
        end_date: str = "",
        top_n: int = 20
    ) -> str:
        """
        分析申万二级行业Alpha排名上升速度
        
        参数:
            benchmark_code: 基准指数代码（默认：000001.SH上证指数）
            end_date: 结束日期（YYYYMMDD格式，默认今天）
            top_n: 显示前N名（默认20）
        
        返回:
            包含排名上升速度的格式化字符串，包括：
            - 行业当天alpha值
            - 相较昨日上升位数
            - 相较前天上升位数
            - 一天内上升位数排行
            - 两天内上升位数排行
        
        说明:
            - 自动分析所有已发布指数的申万二级行业
            - 计算排名上升速度（当天对比前一天和前两天的排名变化）
            - 正数表示排名上升，负数表示排名下降
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 如果end_date为空，使用None让calculate_alpha_rank_velocity使用默认值
            if end_date == "":
                end_date = None
            
            # 申万二级行业代码列表
            sector_codes = get_l2_sector_codes()
            
            # 计算排名上升速度
            df = calculate_alpha_rank_velocity(sector_codes, benchmark_code, end_date)
            
            if df.empty:
                return "无法获取板块数据，请检查网络连接和token配置。\n提示：如果所有板块都返回错误，可能是数据获取失败，请检查Tushare token是否有效。"
            
            # 获取实际使用的日期信息
            current_date = df.attrs.get('current_date', '未知')
            yesterday_date = df.attrs.get('yesterday_date', None)
            day_before_yesterday_date = df.attrs.get('day_before_yesterday_date', None)
            
            # 格式化日期显示
            def format_date_display(date_str):
                """格式化日期显示（YYYYMMDD -> YYYY-MM-DD）"""
                if date_str and len(date_str) == 8:
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                return date_str
            
            current_date_display = format_date_display(current_date)
            yesterday_date_display = format_date_display(yesterday_date) if yesterday_date else "无数据"
            day_before_yesterday_date_display = format_date_display(day_before_yesterday_date) if day_before_yesterday_date else "无数据"
            
            # 检查是否需要说明日期调整
            # 如果end_date不为None，需要检查是否与实际使用的current_date不同
            date_note = ""
            original_end_date = end_date  # 保存原始的end_date用于比较
            if original_end_date is None:
                # 如果end_date是None，说明用户没有指定日期，使用今天
                from datetime import datetime
                original_end_date = datetime.now().strftime('%Y%m%d')
            
            if str(original_end_date) != str(current_date):
                # 如果指定的日期与实际使用的current_date不同，说明指定日期不是交易日
                original_end_date_display = format_date_display(str(original_end_date))
                date_note = f"\n  ⚠️ 注意：指定日期 {original_end_date_display} 不是交易日，已自动使用最近的交易日 {current_date_display}"
            
            # 格式化输出
            output = []
            output.append("📊 申万二级行业Alpha排名上升速度分析")
            output.append("=" * 120)
            output.append("")
            output.append(f"📅 分析日期：")
            output.append(f"  - 当前日期：{current_date_display} ({current_date})")
            output.append(f"  - 对比日期1（较昨日）：{yesterday_date_display} ({yesterday_date if yesterday_date else '无数据'})")
            output.append(f"  - 对比日期2（较前天）：{day_before_yesterday_date_display} ({day_before_yesterday_date if day_before_yesterday_date else '无数据'})")
            if date_note:
                output.append(date_note)
            output.append("")
            
            # 显示所有行业的基本信息
            output.append("📈 所有行业Alpha值及排名变化：")
            output.append("-" * 120)
            # 使用实际日期替换"当天"、"较昨日"、"较前天"
            change_1d_label = f"较{yesterday_date_display}变化" if yesterday_date else "较昨日上升"
            change_2d_label = f"较{day_before_yesterday_date_display}变化" if day_before_yesterday_date else "较前天上升"
            output.append(f"{'排名':<6} {'行业代码':<12} {'Alpha值':<12} {change_1d_label:<20} {change_2d_label:<20}")
            output.append("-" * 120)
            
            # 按当前排名排序
            df_sorted = df.sort_values('current_rank', ascending=True)
            
            for _, row in df_sorted.iterrows():
                rank = f"{int(row['current_rank'])}"
                sector_code = row['sector_code']
                alpha = f"{row['current_alpha']*100:.2f}%" if pd.notna(row['current_alpha']) else "-"
                
                # 较昨日上升位数
                if pd.notna(row['rank_change_1d']):
                    change_1d = f"{int(row['rank_change_1d']):+d}"
                    if row['rank_change_1d'] > 0:
                        change_1d += " ⬆️"
                    elif row['rank_change_1d'] < 0:
                        change_1d += " ⬇️"
                    else:
                        change_1d += " ➖"
                else:
                    change_1d = "-"
                
                # 较前天上升位数
                if pd.notna(row['rank_change_2d']):
                    change_2d = f"{int(row['rank_change_2d']):+d}"
                    if row['rank_change_2d'] > 0:
                        change_2d += " ⬆️"
                    elif row['rank_change_2d'] < 0:
                        change_2d += " ⬇️"
                    else:
                        change_2d += " ➖"
                else:
                    change_2d = "-"
                
                output.append(f"{rank:<6} {sector_code:<12} {alpha:<12} {change_1d:<12} {change_2d:<12}")
            
            output.append("")
            
            # 一天内上升位数排行（只显示有数据的）
            df_1d = df[df['rank_change_1d'].notna()].copy()
            if not df_1d.empty:
                df_1d = df_1d.sort_values('rank_change_1d', ascending=False)
                output.append(f"🚀 较{yesterday_date_display}排名变化排行（前10名）：")
                output.append("-" * 120)
                output.append(f"{'排名':<6} {'行业代码':<12} {f'{current_date_display}排名':<15} {'排名变化':<12} {'Alpha值':<12}")
                output.append("-" * 120)
                
                for idx, (_, row) in enumerate(df_1d.head(10).iterrows(), 1):
                    rank = f"{int(row['current_rank'])}"
                    sector_code = row['sector_code']
                    change_1d = f"{int(row['rank_change_1d']):+d}"
                    alpha = f"{row['current_alpha']*100:.2f}%" if pd.notna(row['current_alpha']) else "-"
                    output.append(f"{idx:<6} {sector_code:<12} {rank:<15} {change_1d:<12} {alpha:<12}")
                
                output.append("")
            
            # 两天内上升位数排行（只显示有数据的）
            df_2d = df[df['rank_change_2d'].notna()].copy()
            if not df_2d.empty:
                df_2d = df_2d.sort_values('rank_change_2d', ascending=False)
                output.append(f"🚀 较{day_before_yesterday_date_display}排名变化排行（前10名）：")
                output.append("-" * 120)
                output.append(f"{'排名':<6} {'行业代码':<12} {f'{current_date_display}排名':<15} {'排名变化':<12} {'Alpha值':<12}")
                output.append("-" * 120)
                
                for idx, (_, row) in enumerate(df_2d.head(10).iterrows(), 1):
                    rank = f"{int(row['current_rank'])}"
                    sector_code = row['sector_code']
                    change_2d = f"{int(row['rank_change_2d']):+d}"
                    alpha = f"{row['current_alpha']*100:.2f}%" if pd.notna(row['current_alpha']) else "-"
                    output.append(f"{idx:<6} {sector_code:<12} {rank:<15} {change_2d:<12} {alpha:<12}")
                
                output.append("")
            
            output.append("📝 说明：")
            output.append("  - Alpha = 板块收益率 - 基准收益率（上证指数）")
            output.append("  - 排名变化 = 对比日期排名 - 当前排名（正数表示排名上升）")
            output.append(f"  - 当前日期：{current_date_display} ({current_date})")
            if yesterday_date:
                output.append(f"  - 对比日期1：{yesterday_date_display} ({yesterday_date})")
            if day_before_yesterday_date:
                output.append(f"  - 对比日期2：{day_before_yesterday_date_display} ({day_before_yesterday_date})")
            output.append("  - 建议关注排名变化较大的行业，可能具有较强动能")
            output.append("")
            output.append(f"📊 统计：共分析 {len(df)} 个二级行业")
            
            return "\n".join(output)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def rank_l1_sectors_alpha_full(
        benchmark_code: str = "000001.SH",
        end_date: str = ""
    ) -> str:
        """
        获取申万一级行业Alpha综合得分完整排行
        
        参数:
            benchmark_code: 基准指数代码（默认：000001.SH上证指数）
            end_date: 结束日期（YYYYMMDD格式，默认今天）
        
        返回:
            包含所有申万一级行业Alpha综合得分排行的格式化字符串
        
        说明:
            - 自动分析所有31个申万一级行业
            - 按综合得分降序排列
            - 显示所有行业的完整排名
        """
        # 如果end_date为空，使用None让analyze_sector_alpha使用默认值
        if end_date == "":
            end_date = None
        
        # 申万一级行业代码列表
        sector_codes = get_l1_sector_codes()
        
        df = rank_sectors_alpha(sector_codes, benchmark_code, end_date)
        
        if df.empty:
            return "无法获取板块数据，请检查网络连接和token配置。\n提示：如果所有板块都返回错误，可能是数据获取失败，请检查Tushare token是否有效。"
        
        # 添加行业名称
        name_map = get_l1_sector_name_map()
        df['name'] = df['sector_code'].map(name_map).fillna(df['sector_code'])
        
        # 显示所有排名
        result = format_alpha_analysis(df)
        
        result += f"\n\n（共 {len(df)} 个一级行业，已显示全部）"
        
        return result
    
    @mcp.tool()
    def rank_l1_sectors_alpha_velocity(
        benchmark_code: str = "000001.SH",
        end_date: str = ""
    ) -> str:
        """
        分析申万一级行业Alpha排名上升速度
        
        参数:
            benchmark_code: 基准指数代码（默认：000001.SH上证指数）
            end_date: 结束日期（YYYYMMDD格式，默认今天）
        
        返回:
            包含排名上升速度的格式化字符串，包括：
            - 行业当天alpha值
            - 相较昨日上升位数
            - 相较前天上升位数
            - 一天内上升位数排行
            - 两天内上升位数排行
        
        说明:
            - 自动分析所有31个申万一级行业
            - 计算排名上升速度（当天对比前一天和前两天的排名变化）
            - 正数表示排名上升，负数表示排名下降
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 如果end_date为空，使用None让calculate_alpha_rank_velocity使用默认值
            if end_date == "":
                end_date = None
            
            # 申万一级行业代码列表
            sector_codes = get_l1_sector_codes()
            
            # 计算排名上升速度
            df = calculate_alpha_rank_velocity(sector_codes, benchmark_code, end_date)
            
            if df.empty:
                # 如果无法获取排名上升速度数据，尝试获取当前排名作为降级方案
                from tools.alpha_strategy_analyzer import rank_sectors_alpha
                from datetime import datetime
                today = datetime.now().strftime('%Y%m%d')
                df_current = rank_sectors_alpha(sector_codes, benchmark_code, today)
                if not df_current.empty:
                    # 返回当前排名，但提示无法获取历史排名
                    return f"⚠️ 无法获取历史排名数据，仅显示当前排名：\n\n" + format_alpha_analysis(df_current) + "\n\n提示：可能是API限流或历史数据缺失，请稍后重试获取排名上升速度分析。"
                else:
                    return "无法获取板块数据，请检查网络连接和token配置。\n提示：如果所有板块都返回错误，可能是数据获取失败，请检查Tushare token是否有效。"
            
            # 获取实际使用的日期信息
            current_date = df.attrs.get('current_date', '未知')
            yesterday_date = df.attrs.get('yesterday_date', None)
            day_before_yesterday_date = df.attrs.get('day_before_yesterday_date', None)
            
            # 格式化日期显示
            def format_date_display(date_str):
                """格式化日期显示（YYYYMMDD -> YYYY-MM-DD）"""
                if date_str and len(date_str) == 8:
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                return date_str
            
            current_date_display = format_date_display(current_date)
            yesterday_date_display = format_date_display(yesterday_date) if yesterday_date else "无数据"
            day_before_yesterday_date_display = format_date_display(day_before_yesterday_date) if day_before_yesterday_date else "无数据"
            
            # 检查是否需要说明日期调整
            # 如果end_date不为None，需要检查是否与实际使用的current_date不同
            date_note = ""
            original_end_date = end_date  # 保存原始的end_date用于比较
            if original_end_date is None:
                # 如果end_date是None，说明用户没有指定日期，使用今天
                from datetime import datetime
                original_end_date = datetime.now().strftime('%Y%m%d')
            
            if str(original_end_date) != str(current_date):
                # 如果指定的日期与实际使用的current_date不同，说明指定日期不是交易日
                original_end_date_display = format_date_display(str(original_end_date))
                date_note = f"\n  ⚠️ 注意：指定日期 {original_end_date_display} 不是交易日，已自动使用最近的交易日 {current_date_display}"
            
            # 格式化输出
            output = []
            output.append("📊 申万一级行业Alpha排名上升速度分析")
            output.append("=" * 120)
            output.append("")
            output.append(f"📅 分析日期：")
            output.append(f"  - 当前日期：{current_date_display} ({current_date})")
            output.append(f"  - 对比日期1（较昨日）：{yesterday_date_display} ({yesterday_date if yesterday_date else '无数据'})")
            output.append(f"  - 对比日期2（较前天）：{day_before_yesterday_date_display} ({day_before_yesterday_date if day_before_yesterday_date else '无数据'})")
            if date_note:
                output.append(date_note)
            output.append("")
            
            # 显示所有行业的基本信息
            output.append("📈 所有行业Alpha值及排名变化：")
            output.append("-" * 120)
            # 使用实际日期替换"当天"、"较昨日"、"较前天"
            change_1d_label = f"较{yesterday_date_display}变化" if yesterday_date else "较昨日上升"
            change_2d_label = f"较{day_before_yesterday_date_display}变化" if day_before_yesterday_date else "较前天上升"
            output.append(f"{'排名':<6} {'行业代码':<12} {'Alpha值':<12} {change_1d_label:<20} {change_2d_label:<20}")
            output.append("-" * 120)
            
            # 按当前排名排序
            df_sorted = df.sort_values('current_rank', ascending=True)
            
            for _, row in df_sorted.iterrows():
                rank = f"{int(row['current_rank'])}"
                sector_code = row['sector_code']
                alpha = f"{row['current_alpha']*100:.2f}%" if pd.notna(row['current_alpha']) else "-"
                
                # 较昨日上升位数
                if pd.notna(row['rank_change_1d']):
                    change_1d = f"{int(row['rank_change_1d']):+d}"
                    if row['rank_change_1d'] > 0:
                        change_1d += " ⬆️"
                    elif row['rank_change_1d'] < 0:
                        change_1d += " ⬇️"
                    else:
                        change_1d += " ➖"
                else:
                    change_1d = "-"
                
                # 较前天上升位数
                if pd.notna(row['rank_change_2d']):
                    change_2d = f"{int(row['rank_change_2d']):+d}"
                    if row['rank_change_2d'] > 0:
                        change_2d += " ⬆️"
                    elif row['rank_change_2d'] < 0:
                        change_2d += " ⬇️"
                    else:
                        change_2d += " ➖"
                else:
                    change_2d = "-"
                
                output.append(f"{rank:<6} {sector_code:<12} {alpha:<12} {change_1d:<12} {change_2d:<12}")
            
            output.append("")
            
            # 一天内上升位数排行（只显示有数据的）
            df_1d = df[df['rank_change_1d'].notna()].copy()
            if not df_1d.empty:
                df_1d = df_1d.sort_values('rank_change_1d', ascending=False)
                output.append(f"🚀 较{yesterday_date_display}排名变化排行（前10名）：")
                output.append("-" * 120)
                output.append(f"{'排名':<6} {'行业代码':<12} {f'{current_date_display}排名':<15} {'排名变化':<12} {'Alpha值':<12}")
                output.append("-" * 120)
                
                for idx, (_, row) in enumerate(df_1d.head(10).iterrows(), 1):
                    rank = f"{int(row['current_rank'])}"
                    sector_code = row['sector_code']
                    change_1d = f"{int(row['rank_change_1d']):+d}"
                    alpha = f"{row['current_alpha']*100:.2f}%" if pd.notna(row['current_alpha']) else "-"
                    output.append(f"{idx:<6} {sector_code:<12} {rank:<15} {change_1d:<12} {alpha:<12}")
                
                output.append("")
            
            # 两天内上升位数排行（只显示有数据的）
            df_2d = df[df['rank_change_2d'].notna()].copy()
            if not df_2d.empty:
                df_2d = df_2d.sort_values('rank_change_2d', ascending=False)
                output.append(f"🚀 较{day_before_yesterday_date_display}排名变化排行（前10名）：")
                output.append("-" * 120)
                output.append(f"{'排名':<6} {'行业代码':<12} {f'{current_date_display}排名':<15} {'排名变化':<12} {'Alpha值':<12}")
                output.append("-" * 120)
                
                for idx, (_, row) in enumerate(df_2d.head(10).iterrows(), 1):
                    rank = f"{int(row['current_rank'])}"
                    sector_code = row['sector_code']
                    change_2d = f"{int(row['rank_change_2d']):+d}"
                    alpha = f"{row['current_alpha']*100:.2f}%" if pd.notna(row['current_alpha']) else "-"
                    output.append(f"{idx:<6} {sector_code:<12} {rank:<15} {change_2d:<12} {alpha:<12}")
                
                output.append("")
            
            output.append("📝 说明：")
            output.append("  - Alpha = 板块收益率 - 基准收益率（上证指数）")
            output.append("  - 排名变化 = 对比日期排名 - 当前排名（正数表示排名上升）")
            output.append(f"  - 当前日期：{current_date_display} ({current_date})")
            if yesterday_date:
                output.append(f"  - 对比日期1：{yesterday_date_display} ({yesterday_date})")
            if day_before_yesterday_date:
                output.append(f"  - 对比日期2：{day_before_yesterday_date_display} ({day_before_yesterday_date})")
            output.append("  - 建议关注排名变化较大的行业，可能具有较强动能")
            output.append("")
            output.append(f"📊 统计：共分析 {len(df)} 个一级行业")
            
            return "\n".join(output)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"


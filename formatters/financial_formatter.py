"""财务报表格式化"""
import pandas as pd

def format_income_statement_analysis(df: pd.DataFrame) -> str:
    """
    格式化利润表分析输出
    
    参数:
        df: 包含利润表数据的DataFrame
    """
    if df.empty:
        return "未找到符合条件的利润表数据"
        
    # 按照报告期末排序
    df = df.sort_values('end_date')
    
    # 提取年份和季度信息
    df['year'] = df['end_date'].str[:4]
    df['quarter'] = df['end_date'].str[4:6].map({'03': 'Q1', '06': 'Q2', '09': 'Q3', '12': 'Q4'})
    df['period'] = df['year'] + df['quarter']
    
    # 准备表头
    header = ["项目"] + df['period'].tolist()
    
    # 准备数据行
    rows = []
    metrics = {
        'total_revenue': '营业总收入',
        'revenue': '营业收入',
        'total_cogs': '营业总成本',
        'oper_cost': '营业成本',
        'sell_exp': '销售费用',
        'admin_exp': '管理费用',
        'fin_exp': '财务费用',
        'operate_profit': '营业利润',
        'total_profit': '利润总额',
        'n_income': '净利润',
        'basic_eps': '每股收益'
    }
    
    for key, name in metrics.items():
        row = [name]
        for _, period_data in df.iterrows():
            value = period_data[key]
            # 格式化数值（单位：亿元）
            if key != 'basic_eps':
                value = f"{float(value)/100000000:.2f}亿" if pd.notna(value) else '-'
            else:
                value = f"{float(value):.2f}" if pd.notna(value) else '-'
            row.append(value)
        rows.append(row)
    
    # 生成表格
    table = []
    table.append(" | ".join([f"{col:^12}" for col in header]))
    table.append("-" * (14 * len(header)))
    for row in rows:
        table.append(" | ".join([f"{col:^12}" for col in row]))
    
    # 计算同比增长率
    def calc_yoy(series):
        if len(series) >= 2:
            return (series.iloc[-1] - series.iloc[-2]) / abs(series.iloc[-2]) * 100
        return None
    
    # 计算环比增长率
    def calc_qoq(series):
        if len(series) >= 2:
            return (series.iloc[-1] - series.iloc[-2]) / abs(series.iloc[-2]) * 100
        return None
    
    # 生成分析报告
    analysis = []
    analysis.append("\n📊 财务分析报告")
    analysis.append("=" * 50)
    
    # 1. 收入分析
    analysis.append("\n一、收入分析")
    analysis.append("-" * 20)
    
    # 1.1 营收规模与增长
    revenue_yoy = calc_yoy(df['total_revenue'])
    revenue_qoq = calc_qoq(df['total_revenue'])
    latest_revenue = float(df.iloc[-1]['total_revenue'])/100000000
    
    analysis.append("1. 营收规模与增长：")
    analysis.append(f"   • 当期营收：{latest_revenue:.2f}亿元")
    if revenue_yoy is not None:
        analysis.append(f"   • 同比变动：{revenue_yoy:+.2f}%")
    if revenue_qoq is not None:
        analysis.append(f"   • 环比变动：{revenue_qoq:+.2f}%")
    
    # 2. 盈利能力分析
    analysis.append("\n二、盈利能力分析")
    analysis.append("-" * 20)
    
    # 2.1 利润规模与增长
    latest = df.iloc[-1]
    profit_yoy = calc_yoy(df['n_income'])
    profit_qoq = calc_qoq(df['n_income'])
    latest_profit = float(latest['n_income'])/100000000
    
    analysis.append("1. 利润规模与增长：")
    analysis.append(f"   • 当期净利润：{latest_profit:.2f}亿元")
    if profit_yoy is not None:
        analysis.append(f"   • 同比变动：{profit_yoy:+.2f}%")
    if profit_qoq is not None:
        analysis.append(f"   • 环比变动：{profit_qoq:+.2f}%")
    
    # 2.2 盈利能力指标
    gross_margin = ((latest['total_revenue'] - latest['oper_cost']) / latest['total_revenue']) * 100
    operating_margin = (latest['operate_profit'] / latest['total_revenue']) * 100
    net_margin = (latest['n_income'] / latest['total_revenue']) * 100
    
    analysis.append("\n2. 盈利能力指标：")
    analysis.append(f"   • 毛利率：{gross_margin:.2f}%")
    analysis.append(f"   • 营业利润率：{operating_margin:.2f}%")
    analysis.append(f"   • 净利润率：{net_margin:.2f}%")
    
    # 3. 成本费用分析
    analysis.append("\n三、成本费用分析")
    analysis.append("-" * 20)
    
    # 3.1 成本费用结构
    total_revenue = float(latest['total_revenue'])
    cost_structure = {
        '营业成本': (latest['oper_cost'] / total_revenue) * 100,
        '销售费用': (latest['sell_exp'] / total_revenue) * 100,
        '管理费用': (latest['admin_exp'] / total_revenue) * 100,
        '财务费用': (latest['fin_exp'] / total_revenue) * 100
    }
    
    analysis.append("1. 成本费用结构（占营收比）：")
    for item, ratio in cost_structure.items():
        analysis.append(f"   • {item}率：{ratio:.2f}%")
    
    # 3.2 费用变动分析
    analysis.append("\n2. 主要费用同比变动：")
    expense_items = {
        '销售费用': ('sell_exp', calc_yoy(df['sell_exp'])),
        '管理费用': ('admin_exp', calc_yoy(df['admin_exp'])),
        '财务费用': ('fin_exp', calc_yoy(df['fin_exp']))
    }
    
    for name, (_, yoy) in expense_items.items():
        if yoy is not None:
            analysis.append(f"   • {name}：{yoy:+.2f}%")
    
    # 4. 每股指标
    analysis.append("\n四、每股指标")
    analysis.append("-" * 20)
    latest_eps = float(latest['basic_eps'])
    eps_yoy = calc_yoy(df['basic_eps'])
    
    analysis.append(f"• 基本每股收益：{latest_eps:.4f}元")
    if eps_yoy is not None:
        analysis.append(f"• 同比变动：{eps_yoy:+.2f}%")
    
    # 5. 风险提示
    analysis.append("\n⚠️ 风险提示")
    analysis.append("-" * 20)
    analysis.append("以上分析基于历史财务数据，仅供参考。投资决策需考虑更多因素，包括但不限于：")
    analysis.append("• 行业周期与竞争态势")
    analysis.append("• 公司经营与治理状况")
    analysis.append("• 宏观经济环境")
    analysis.append("• 政策法规变化")
    
    return "\n".join(table) + "\n\n" + "\n".join(analysis)


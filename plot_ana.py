import akshare as ak
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
import time
import random
import os
import numpy as np

# ================= 0. 确保所需库已安装 =================
# 建议运行前执行：pip install --upgrade akshare plotly pandas requests

# ================= 0.1 定义彩色打印函数 =================
def print_red(text):
    print(f"\033[91m{text}\033[0m")

def print_green(text):
    print(f"\033[92m{text}\033[0m")

def print_yellow(text):
    print(f"\033[93m{text}\033[0m")

# ================= 1. 定义标的 =================
tickers = {
    '阿里巴巴': {'type': 'us', 'symbol': 'BABA', 'display': '阿里巴巴', 'csv': 'BABA.csv'},
    '腾讯': {'type': 'hk', 'symbol': '00700', 'display': '腾讯', 'csv': '00700.csv'},
    '拼多多': {'type': 'us', 'symbol': 'PDD', 'display': '拼多多', 'csv': 'PDD.csv'},
    '京东': {'type': 'us', 'symbol': 'JD', 'display': '京东', 'csv': 'JD.csv'},
    '贵州茅台': {'type': 'cn', 'symbol': '600519', 'display': '贵州茅台', 'csv': '600519.csv'},
    '招商银行': {'type': 'cn', 'symbol': '600036', 'display': '招商银行', 'csv': '600036.csv'},
    '中国神华': {'type': 'hk', 'symbol': '01088', 'display': '中国神华', 'csv': '01088.csv'},
    '黄金ETF': {'type': 'etf', 'symbols': ['518880', '518600', '518850'], 'display': '黄金ETF', 'csv': 'gold_etf.csv'},
    '可口可乐': {'type': 'us', 'symbol': 'KO', 'display': '可口可乐', 'csv': 'KO.csv'},
    'Coke Consolidated': {'type': 'us', 'symbol': 'COKE', 'display': 'Coke Consolidated', 'csv': 'COKE.csv'},
    '白银': {'type': 'silver', 'symbols': ['SI'], 'display': '白银', 'csv': 'silver.csv'}  # 期货代码
}

# ================= 2. 设置时间范围 =================
start_date = "20240101"
end_date = datetime.today().strftime('%Y%m%d')
print(f"数据下载时间范围：{start_date} 至 {end_date}")

# ================= 3. 带重试的数据下载函数 =================
def download_with_retry(func, max_retries=3, base_delay=5):
    for attempt in range(max_retries):
        try:
            delay = base_delay * (attempt + 1) + random.uniform(2, 5)
            if attempt > 0:
                print(f"  第{attempt+1}次尝试，等待{delay:.1f}秒...")
                time.sleep(delay)
            result = func()
            if result is not None and not result.empty:
                return result
            print(f"  第{attempt+1}次尝试：返回空数据")
        except Exception as e:
            print(f"  第{attempt+1}次尝试失败：{e}")
    return None

# ================= 4. 数据下载函数 =================
def download_and_save(csv_path, download_func, symbol_display):
    print(f"  🌐 开始下载 {symbol_display}...")
    series = download_func()
    if series is not None:
        if len(series) < 5:
            print_yellow(f"  ⚠️ {symbol_display} 数据量过少 ({len(series)} 条)")
        try:
            df_save = pd.DataFrame({symbol_display: series})
            df_save.to_csv(csv_path)
            print_green(f"  ✅ {symbol_display} 下载成功，共 {len(series)} 条数据")
        except Exception as e:
            print(f"  ⚠️ CSV保存失败: {e}")
    else:
        print_red(f"  ❌ {symbol_display} 下载失败")
    return series

# ================= 5. 数据获取函数 =================
def fetch_us_stock(symbol, display_name):
    def _fetch():
        df = ak.stock_us_daily(symbol=symbol, adjust="qfq")
        if df.empty:
            return None
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df = df[(df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))]
        return df['close']
    return _fetch

def fetch_cn_stock(symbol, display_name):
    def _fetch():
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        if df.empty:
            return None
        df['日期'] = pd.to_datetime(df['日期'])
        df.set_index('日期', inplace=True)
        return df['收盘']
    return _fetch

def fetch_hk_stock(symbol, display_name):
    def _fetch():
        sym = symbol.zfill(5)
        # 方法1: stock_hk_daily
        try:
            print(f"    尝试新浪港股接口...")
            df = ak.stock_hk_daily(symbol=sym, adjust="qfq")
            if not df.empty:
                df['日期'] = pd.to_datetime(df['日期'])
                df.set_index('日期', inplace=True)
                df = df[(df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))]
                if not df.empty:
                    print(f"      新浪接口成功，获取 {len(df)} 条数据")
                    return df['收盘']
        except Exception as e:
            print(f"      新浪接口失败: {e}")
        # 方法2: stock_hk_hist
        try:
            print(f"    尝试东方财富港股接口...")
            df = ak.stock_hk_hist(symbol=sym, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            if not df.empty:
                df['日期'] = pd.to_datetime(df['日期'])
                df.set_index('日期', inplace=True)
                print(f"      东方财富接口成功，获取 {len(df)} 条数据")
                return df['收盘']
        except Exception as e:
            print(f"      东方财富接口失败: {e}")
        return None
    return _fetch

def fetch_etf_data(symbol, display_name):
    def _fetch():
        try:
            df = ak.fund_etf_hist_em(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            if not df.empty:
                if '日期' in df.columns:
                    df['日期'] = pd.to_datetime(df['日期'])
                    df.set_index('日期', inplace=True)
                    return df['收盘']
                elif 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                    return df['close']
        except Exception as e:
            print(f"    ETF获取失败: {e}")
            try:
                print(f"    尝试基金净值接口...")
                df2 = ak.fund_em_open_fund_info(fund=symbol, indicator="单位净值走势")
                if not df2.empty:
                    df2['净值日期'] = pd.to_datetime(df2['净值日期'])
                    df2.set_index('净值日期', inplace=True)
                    df2 = df2[(df2.index >= pd.to_datetime(start_date)) & (df2.index <= pd.to_datetime(end_date))]
                    return df2['单位净值']
            except Exception as e2:
                print(f"    基金净值接口失败: {e2}")
        return None
    return _fetch

def fetch_silver_data(symbol, display_name):
    """获取白银期货数据 (COMEX白银)"""
    def _fetch():
        try:
            print(f"    尝试COMEX白银期货 {symbol}...")
            df = ak.futures_foreign_hist(symbol=symbol, start_date=start_date, end_date=end_date)
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                print(f"      COMEX白银成功，获取 {len(df)} 条数据")
                return df['close']
        except Exception as e:
            print(f"    COMEX白银失败: {e}")
        return None
    return _fetch

# ================= 6. 下载所有数据 =================
data_series = {}
failed_tickers = []
used_symbols = {}

# 下载阿里巴巴
print("\n" + "="*50)
print("开始下载阿里巴巴 (美股)")
print("="*50)
ali_series = download_and_save('BABA.csv', fetch_us_stock('BABA', '阿里巴巴'), '阿里巴巴')
if ali_series is None:
    print_red("❌ 阿里巴巴数据下载失败，无法继续。")
    sys.exit(1)
data_series['阿里巴巴'] = ali_series
used_symbols['阿里巴巴'] = 'BABA'

# 下载其他标的
for display_name, info in tickers.items():
    if display_name == '阿里巴巴':
        continue
    print("\n" + "="*50)
    print(f"开始下载 {display_name}")
    print("="*50)

    if info['type'] == 'us':
        series = download_and_save(info['csv'], fetch_us_stock(info['symbol'], display_name), display_name)
        if series is not None:
            used_symbols[display_name] = info['symbol']
            data_series[display_name] = series
        else:
            failed_tickers.append(display_name)

    elif info['type'] == 'cn':
        series = download_and_save(info['csv'], fetch_cn_stock(info['symbol'], display_name), display_name)
        if series is not None:
            used_symbols[display_name] = info['symbol']
            data_series[display_name] = series
        else:
            failed_tickers.append(display_name)

    elif info['type'] == 'hk':
        time.sleep(random.uniform(3, 5))
        series = download_and_save(info['csv'], fetch_hk_stock(info['symbol'], display_name), display_name)
        if series is not None:
            used_symbols[display_name] = info['symbol']
            data_series[display_name] = series
        else:
            failed_tickers.append(display_name)

    elif info['type'] == 'etf':
        success = False
        for sym in info['symbols']:
            print(f"\n  尝试代码 {sym} ...")
            series = download_and_save(f"{sym}.csv", fetch_etf_data(sym, display_name), f"{display_name}({sym})")
            if series is not None:
                used_symbols[display_name] = sym
                data_series[display_name] = series
                success = True
                break
            else:
                time.sleep(random.uniform(3, 6))
        if not success:
            failed_tickers.append(display_name)

    elif info['type'] == 'silver':
        success = False
        for sym in info['symbols']:
            print(f"\n  尝试白银代码 {sym} ...")
            series = download_and_save(info['csv'], fetch_silver_data(sym, display_name), f"{display_name}({sym})")
            if series is not None:
                used_symbols[display_name] = sym
                data_series[display_name] = series
                success = True
                break
            else:
                time.sleep(random.uniform(3, 6))
        if not success:
            failed_tickers.append(display_name)

# ================= 下载完成总提示 =================
print("\n" + "="*50)
print("数据下载阶段总结")
print("="*50)
if failed_tickers:
    print_red(f"❌ 以下标的下载失败: {failed_tickers}")
else:
    print_green("✅ 所有标的均下载成功！")
print()  # 空一行

# ================= 7. 数据对齐与清洗 =================
if '阿里巴巴' not in data_series:
    print_red("❌ 错误：缺少阿里巴巴数据")
    sys.exit(1)

base_index = data_series['阿里巴巴'].index
print(f"阿里巴巴共有 {len(base_index)} 个交易日")

aligned_data = {}
for name, series in data_series.items():
    aligned = series.reindex(base_index)
    non_null_count = aligned.count()
    if non_null_count < 5:
        print_yellow(f"⚠️ {name} 有效数据仅 {non_null_count} 条，已排除")
        if name != '阿里巴巴':
            failed_tickers.append(name)
        continue
    aligned_data[name] = aligned

if not aligned_data:
    print_red("❌ 没有足够的数据进行分析")
    sys.exit(1)

data = pd.DataFrame(aligned_data)
print(f"\n对齐后数据形状: {data.shape}")

# 保存对齐后的原始数据
aligned_csv_path = "aligned_data.csv"
data.to_csv(aligned_csv_path)
print_green(f"✅ 对齐后数据已保存到 {aligned_csv_path}")

# 检查并排除常数列（标准差为0）和全0列
invalid_cols = []
for col in data.columns:
    if data[col].std() == 0 or (data[col] == 0).all():
        print_red(f"❌ 列 '{col}' 为常数或全0，已排除")
        invalid_cols.append(col)
if invalid_cols:
    data = data.drop(columns=invalid_cols)
    for col in invalid_cols:
        if col != '阿里巴巴':
            failed_tickers.append(col)

if data.empty:
    print_red("❌ 无有效数据")
    sys.exit(1)

# 插值填充
data = data.interpolate(method='linear', limit_area='inside')
data = data.ffill().bfill()

# 删除任何剩余NaN行
if data.isnull().any().any():
    print_yellow("⚠️ 仍有NaN，将删除包含NaN的行")
    data = data.dropna()

if len(data) < 2:
    print_red(f"❌ 有效交易日只有 {len(data)} 天，无法计算收益率")
    sys.exit(1)

print(f"最终数据共 {len(data)} 个交易日，包含 {len(data.columns)} 个标的：{list(data.columns)}")

# ================= 8. 计算相关性 =================
returns = data.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
if returns.empty:
    print_red("❌ 收益率计算后无有效数据")
    print("数据前5行：")
    print(data.head())
    sys.exit(1)

corr_matrix = returns.corr()
print("\n=== 各标的日收益率相关性矩阵 ===")
print(corr_matrix.round(3))

print(f"\n与阿里巴巴相关性从高到低排序：")
if '阿里巴巴' in corr_matrix.columns:
    print(corr_matrix['阿里巴巴'].sort_values(ascending=False))
else:
    print_red("阿里巴巴不在相关性矩阵中")

# ================= 9. 归一化并生成HTML图表 =================
normalized = data.div(data.iloc[0]) * 100

fig = go.Figure()
for col in normalized.columns:
    if col == '阿里巴巴':
        corr_val = 1.0
    else:
        corr_val = corr_matrix.loc['阿里巴巴', col] if '阿里巴巴' in corr_matrix.index else float('nan')
    code = used_symbols.get(col, '')
    legend_name = f"{col} ({code}) 相关:{corr_val:.2f}" if code else f"{col} 相关:{corr_val:.2f}"
    fig.add_trace(go.Scatter(x=normalized.index, y=normalized[col], mode='lines', name=legend_name, line=dict(width=2.5)))

fig.update_layout(
    title={'text': '阿里巴巴与低相关性标的走势对比 (起始日 = 100)', 'x': 0.5, 'xanchor': 'center',
           'font': dict(size=20, family='Arial Black', color='black', weight='bold')},
    xaxis_title='日期', yaxis_title='归一化价格 (起始日 = 100)',
    hovermode='x unified',
    legend=dict(yanchor="top", y=0.99, xanchor="center", x=0.5, font=dict(size=12, weight='bold')),
    template='plotly_white', autosize=True, margin=dict(l=40, r=40, t=80, b=40)
)

# ================= 10. 保存HTML文件 =================
html_filename = "plot_ana.html"
fig.write_html(html_filename)
print_green(f"\n✅ 图表已保存为: {html_filename}")

# 最终失败提示
if failed_tickers:
    print_red(f"\n⚠️ 以下标的最终被排除（下载失败或数据无效）：{failed_tickers}")

print("\n📊 提示：生成的HTML文件可以用浏览器打开查看交互式图表")
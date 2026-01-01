import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
import re
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Set style for business presentations
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 11

# Create charts directory
Path('charts').mkdir(exist_ok=True)

# Read data
df = pd.read_csv('scraped_data.csv')

print(f"Total listings: {len(df)}")
print(f"Columns: {df.columns.tolist()}")

# Clean and prepare data
def clean_price(price_str):
    """Extract numeric price from string"""
    if pd.isna(price_str):
        return None
    price_str = str(price_str).replace(' ', '').replace(',', '')
    match = re.search(r'\d+', price_str)
    return int(match.group()) if match else None

def extract_region(region_str):
    """Extract main region from full location string"""
    if pd.isna(region_str):
        return 'Unknown'
    region_str = str(region_str)
    # Extract first part before comma or special char
    if '‚' in region_str:
        return region_str.split('‚')[0].strip()
    elif ',' in region_str:
        return region_str.split(',')[0].strip()
    return region_str.strip()

def extract_rooms(room_str):
    """Extract number of rooms"""
    if pd.isna(room_str):
        return None
    room_str = str(room_str)
    if 'Studio' in room_str or 'studio' in room_str:
        return 0
    match = re.search(r'(\d+)', room_str)
    if match:
        num = int(match.group(1))
        return num if num < 20 else None  # Filter outliers
    return None

def clean_area(area_str):
    """Extract numeric area"""
    if pd.isna(area_str):
        return None
    area_str = str(area_str).replace(',', '.')
    match = re.search(r'(\d+\.?\d*)', area_str)
    if match:
        area = float(match.group(1))
        return area if 10 < area < 1000 else None  # Filter outliers
    return None

# Clean data
df['price_clean'] = df['price'].apply(clean_price)
df['region_clean'] = df['region'].apply(extract_region)
df['rooms_clean'] = df['room_count'].apply(extract_rooms)
df['area_clean'] = df['area_sqm'].apply(clean_area)

# Extract property type from category
def get_property_type(category):
    if pd.isna(category):
        return 'Other'
    category = str(category).lower()
    if 'yeni tikili' in category:
        return 'New Building'
    elif 'həyət' in category or 'villa' in category or 'bağ' in category:
        return 'House/Villa'
    elif 'kirayə' in category:
        return 'Rental'
    elif 'obyekt' in category or 'ofis' in category:
        return 'Commercial'
    else:
        return 'Apartment'

df['property_type'] = df['category'].apply(get_property_type)

# Filter only listings with prices in AZN
df_azn = df[(df['currency'] == 'azn') & (df['price_clean'].notna()) & (df['price_clean'] > 0)]

print(f"\nListings with valid AZN prices: {len(df_azn)}")
print(f"Price range: {df_azn['price_clean'].min():,.0f} - {df_azn['price_clean'].max():,.0f} AZN")

# === CHART 1: Price Distribution by Property Type ===
plt.figure(figsize=(12, 6))
property_prices = df_azn.groupby('property_type')['price_clean'].agg(['mean', 'median', 'count'])
property_prices = property_prices[property_prices['count'] >= 10].sort_values('median', ascending=False)

x = range(len(property_prices))
width = 0.35

fig, ax = plt.subplots(figsize=(12, 6))
bars1 = ax.bar([i - width/2 for i in x], property_prices['median']/1000, width, label='Median Price', color='#2E86AB')
bars2 = ax.bar([i + width/2 for i in x], property_prices['mean']/1000, width, label='Average Price', color='#A23B72')

ax.set_ylabel('Price (thousand AZN)', fontweight='bold')
ax.set_title('Property Prices by Type - Market Overview', fontweight='bold', fontsize=14)
ax.set_xticks(x)
ax.set_xticklabels(property_prices.index, rotation=45, ha='right')
ax.legend()
ax.grid(axis='y', alpha=0.3)

# Add value labels
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.0f}K',
                ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('charts/01_price_by_property_type.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Generated: Price by Property Type")

# === CHART 2: Market Supply by Region (Top 15) ===
plt.figure(figsize=(14, 6))
region_counts = df['region_clean'].value_counts().head(15)

fig, ax = plt.subplots(figsize=(14, 6))
bars = ax.barh(range(len(region_counts)), region_counts.values, color='#06A77D')
ax.set_yticks(range(len(region_counts)))
ax.set_yticklabels(region_counts.index)
ax.set_xlabel('Number of Listings', fontweight='bold')
ax.set_title('Market Supply by Region - Where Properties Are Listed', fontweight='bold', fontsize=14)
ax.grid(axis='x', alpha=0.3)
ax.invert_yaxis()

# Add value labels
for i, v in enumerate(region_counts.values):
    ax.text(v + 5, i, str(v), va='center', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/02_supply_by_region.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Generated: Supply by Region")

# === CHART 3: Price Distribution by Number of Rooms ===
df_rooms = df_azn[df_azn['rooms_clean'].notna() & (df_azn['rooms_clean'] <= 6)]
if len(df_rooms) > 50:
    plt.figure(figsize=(12, 6))
    room_prices = df_rooms.groupby('rooms_clean')['price_clean'].agg(['median', 'count'])
    room_prices = room_prices[room_prices['count'] >= 10]

    fig, ax1 = plt.subplots(figsize=(12, 6))

    color = '#2E86AB'
    ax1.set_xlabel('Number of Rooms', fontweight='bold')
    ax1.set_ylabel('Median Price (AZN)', color=color, fontweight='bold')
    bars = ax1.bar(room_prices.index, room_prices['median'], color=color, alpha=0.7, label='Median Price')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.grid(axis='y', alpha=0.3)

    # Add second y-axis for count
    ax2 = ax1.twinx()
    color = '#F18F01'
    ax2.set_ylabel('Number of Listings', color=color, fontweight='bold')
    line = ax2.plot(room_prices.index, room_prices['count'], color=color, marker='o', linewidth=2, markersize=8, label='Listing Count')
    ax2.tick_params(axis='y', labelcolor=color)

    # Add value labels on bars
    for i, v in enumerate(room_prices['median']):
        ax1.text(room_prices.index[i], v + 5000, f'{v/1000:.0f}K', ha='center', va='bottom', fontweight='bold')

    plt.title('Property Prices by Number of Rooms', fontweight='bold', fontsize=14)
    fig.tight_layout()
    plt.savefig('charts/03_price_by_rooms.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Generated: Price by Rooms")

# === CHART 4: Listing Engagement Analysis - View Count Insights ===
df_views = df_azn[df_azn['view_count'].notna()].copy()
if len(df_views) > 100:
    # Convert view_count to numeric
    df_views['views_numeric'] = pd.to_numeric(df_views['view_count'], errors='coerce')
    df_views = df_views[df_views['views_numeric'].notna() & (df_views['views_numeric'] > 0)]

    # Create view categories
    view_bins = [0, 100, 500, 1000, 2000, 10000]
    view_labels = ['Low (<100)', 'Medium (100-500)', 'High (500-1K)', 'Very High (1K-2K)', 'Viral (>2K)']
    df_views['view_category'] = pd.cut(df_views['views_numeric'], bins=view_bins, labels=view_labels)

    # Analyze price vs engagement
    view_analysis = df_views.groupby('view_category').agg({
        'price_clean': 'median',
        'elan_id': 'count'
    }).dropna()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Left: Listing count by engagement
    colors1 = ['#E63946', '#F18F01', '#06A77D', '#2E86AB', '#7209B7']
    bars1 = ax1.bar(range(len(view_analysis)), view_analysis['elan_id'], color=colors1[:len(view_analysis)])
    ax1.set_xticks(range(len(view_analysis)))
    ax1.set_xticklabels(view_analysis.index, rotation=45, ha='right')
    ax1.set_ylabel('Number of Listings', fontweight='bold')
    ax1.set_title('Listing Distribution by Engagement Level', fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)

    for i, v in enumerate(view_analysis['elan_id']):
        ax1.text(i, v + max(view_analysis['elan_id'])*0.02, str(int(v)), ha='center', va='bottom', fontweight='bold')

    # Right: Median price by engagement
    bars2 = ax2.bar(range(len(view_analysis)), view_analysis['price_clean']/1000, color=colors1[:len(view_analysis)])
    ax2.set_xticks(range(len(view_analysis)))
    ax2.set_xticklabels(view_analysis.index, rotation=45, ha='right')
    ax2.set_ylabel('Median Price (thousand AZN)', fontweight='bold')
    ax2.set_title('Median Price by Engagement Level', fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)

    for i, v in enumerate(view_analysis['price_clean']):
        ax2.text(i, v/1000 + max(view_analysis['price_clean'])/1000*0.02, f'{v/1000:.0f}K', ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    plt.savefig('charts/04_engagement_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Generated: Engagement Analysis")

# === CHART 5: Market Activity - Listings Over Time ===
df['date_posted_clean'] = pd.to_datetime(df['date_posted'], format='%d %B %Y', errors='coerce')
df_dated = df[df['date_posted_clean'].notna()].copy()

if len(df_dated) > 50:
    df_dated['month'] = df_dated['date_posted_clean'].dt.to_period('M')
    monthly_counts = df_dated.groupby('month').size().sort_index()

    # Convert period to string for plotting
    monthly_counts.index = monthly_counts.index.astype(str)

    plt.figure(figsize=(14, 6))
    plt.plot(range(len(monthly_counts)), monthly_counts.values, marker='o', linewidth=2, markersize=8, color='#06A77D')
    plt.xticks(range(len(monthly_counts)), monthly_counts.index, rotation=45, ha='right')
    plt.ylabel('Number of New Listings', fontweight='bold')
    plt.xlabel('Month', fontweight='bold')
    plt.title('Market Activity Trend - New Listings Over Time', fontweight='bold', fontsize=14)
    plt.grid(True, alpha=0.3)

    # Add value labels on points
    for i, v in enumerate(monthly_counts.values):
        plt.text(i, v + max(monthly_counts.values)*0.02, str(v), ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    plt.savefig('charts/05_listings_over_time.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Generated: Listings Over Time")

# === CHART 6: Property Size Distribution ===
df_size = df_azn[df_azn['area_clean'].notna()]
if len(df_size) > 100:
    # Create size categories
    bins = [0, 50, 75, 100, 150, 200, 500]
    labels = ['<50m²', '50-75m²', '75-100m²', '100-150m²', '150-200m²', '>200m²']
    df_size['size_category'] = pd.cut(df_size['area_clean'], bins=bins, labels=labels)

    size_dist = df_size['size_category'].value_counts().sort_index()

    plt.figure(figsize=(12, 6))
    bars = plt.bar(range(len(size_dist)), size_dist.values, color='#7209B7')
    plt.xticks(range(len(size_dist)), size_dist.index, rotation=45, ha='right')
    plt.ylabel('Number of Properties', fontweight='bold')
    plt.xlabel('Property Size', fontweight='bold')
    plt.title('Property Size Distribution - Market Composition', fontweight='bold', fontsize=14)
    plt.grid(axis='y', alpha=0.3)

    # Add value labels
    for i, v in enumerate(size_dist.values):
        plt.text(i, v + max(size_dist.values)*0.02, str(v), ha='center', va='bottom', fontweight='bold')

    # Add percentage labels
    total = size_dist.sum()
    for i, v in enumerate(size_dist.values):
        pct = (v/total)*100
        plt.text(i, v/2, f'{pct:.1f}%', ha='center', va='center', color='white', fontweight='bold', fontsize=11)

    plt.tight_layout()
    plt.savefig('charts/06_property_size_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Generated: Property Size Distribution")

# === CHART 7: Seller Type Analysis ===
df['seller_type'] = df['advertiser_type'].apply(lambda x:
    'Owner' if pd.notna(x) and 'ƏMLAK sahibi' in str(x)
    else 'Agent' if pd.notna(x) and 'Vasitəçi' in str(x)
    else 'Unknown'
)

seller_counts = df['seller_type'].value_counts()
seller_prices = df_azn.groupby(df['seller_type'])['price_clean'].median()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Count by seller type
bars1 = ax1.bar(range(len(seller_counts)), seller_counts.values, color=['#2E86AB', '#F18F01', '#CCCCCC'])
ax1.set_xticks(range(len(seller_counts)))
ax1.set_xticklabels(seller_counts.index)
ax1.set_ylabel('Number of Listings', fontweight='bold')
ax1.set_title('Listings by Seller Type', fontweight='bold')
ax1.grid(axis='y', alpha=0.3)

for i, v in enumerate(seller_counts.values):
    ax1.text(i, v + max(seller_counts.values)*0.02, str(v), ha='center', va='bottom', fontweight='bold')

# Median price by seller type
seller_prices_filtered = seller_prices[seller_prices.index.isin(['Owner', 'Agent'])]
bars2 = ax2.bar(range(len(seller_prices_filtered)), seller_prices_filtered.values/1000, color=['#2E86AB', '#F18F01'])
ax2.set_xticks(range(len(seller_prices_filtered)))
ax2.set_xticklabels(seller_prices_filtered.index)
ax2.set_ylabel('Median Price (thousand AZN)', fontweight='bold')
ax2.set_title('Median Price by Seller Type', fontweight='bold')
ax2.grid(axis='y', alpha=0.3)

for i, v in enumerate(seller_prices_filtered.values):
    ax2.text(i, v/1000 + max(seller_prices_filtered.values)/1000*0.02, f'{v/1000:.0f}K', ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/07_seller_type_analysis.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Generated: Seller Type Analysis")

# === CHART 8: Price Range Distribution ===
price_bins = [0, 50000, 100000, 150000, 200000, 300000, 500000, 1000000, 5000000]
price_labels = ['<50K', '50-100K', '100-150K', '150-200K', '200-300K', '300-500K', '500K-1M', '>1M']
df_azn['price_range'] = pd.cut(df_azn['price_clean'], bins=price_bins, labels=price_labels)

price_dist = df_azn['price_range'].value_counts().sort_index()

plt.figure(figsize=(12, 6))
bars = plt.bar(range(len(price_dist)), price_dist.values, color='#06A77D')
plt.xticks(range(len(price_dist)), price_dist.index, rotation=45, ha='right')
plt.ylabel('Number of Properties', fontweight='bold')
plt.xlabel('Price Range (AZN)', fontweight='bold')
plt.title('Property Price Range Distribution', fontweight='bold', fontsize=14)
plt.grid(axis='y', alpha=0.3)

# Add value labels
for i, v in enumerate(price_dist.values):
    plt.text(i, v + max(price_dist.values)*0.02, str(v), ha='center', va='bottom', fontweight='bold')

# Add percentage labels
total = price_dist.sum()
for i, v in enumerate(price_dist.values):
    pct = (v/total)*100
    if pct > 2:  # Only show if >2%
        plt.text(i, v/2, f'{pct:.1f}%', ha='center', va='center', color='white', fontweight='bold', fontsize=10)

plt.tight_layout()
plt.savefig('charts/08_price_range_distribution.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Generated: Price Range Distribution")

# === Generate Summary Statistics ===
summary = {
    'total_listings': len(df),
    'valid_prices': len(df_azn),
    'median_price': int(df_azn['price_clean'].median()),
    'average_price': int(df_azn['price_clean'].mean()),
    'price_min': int(df_azn['price_clean'].min()),
    'price_max': int(df_azn['price_clean'].max()),
    'top_regions': df['region_clean'].value_counts().head(5).to_dict(),
    'property_types': df['property_type'].value_counts().to_dict(),
    'seller_distribution': df['seller_type'].value_counts().to_dict()
}

with open('charts/summary_stats.json', 'w', encoding='utf-8') as f:
    json.dump(summary, f, indent=2, ensure_ascii=False)

print("\n" + "="*50)
print("ANALYSIS COMPLETE")
print("="*50)
print(f"Total Listings Analyzed: {summary['total_listings']:,}")
print(f"Median Price: {summary['median_price']:,} AZN")
print(f"Average Price: {summary['average_price']:,} AZN")
print(f"Price Range: {summary['price_min']:,} - {summary['price_max']:,} AZN")
print(f"\nAll charts saved to charts/ directory")
print("Summary statistics saved to charts/summary_stats.json")

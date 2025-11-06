#!/usr/bin/env python3
"""
Comprehensive Chart Generator for Segment Output Files
Version: 1.0.0

This script generates all charts listed in the Chart Suggestions document
for each segment output file in a run.
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import argparse

# Set style for better-looking charts
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class ComprehensiveChartGenerator:
    """Generates comprehensive charts for all segment output files."""
    
    def __init__(self, run_hash: str):
        self.run_hash = run_hash
        # Check if we're in scripts directory or root
        script_dir = Path(__file__).parent
        if (script_dir / "run_logs").exists():
            base_path = script_dir / "run_logs"
        else:
            base_path = Path("run_logs")
        self.base_dir = base_path / run_hash / "outputs"
        self.charts_dir = base_path / run_hash / "outputs" / "insights" / "segment_charts"
        self.charts_dir.mkdir(parents=True, exist_ok=True)
        
        # Track generated charts
        self.generated_charts = {}
        
    def load_csv(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Load a CSV file and return DataFrame or None if not found."""
        try:
            if not file_path.exists():
                print(f"⚠️  File not found: {file_path}")
                return None
            df = pd.read_csv(file_path)
            if df.empty:
                print(f"⚠️  Empty file: {file_path}")
                return None
            return df
        except Exception as e:
            print(f"❌ Error loading {file_path}: {e}")
            return None
    
    def save_chart(self, fig, filename: str, subfolder: str = "") -> str:
        """Save a chart figure and return the path."""
        if subfolder:
            folder = self.charts_dir / subfolder
            folder.mkdir(parents=True, exist_ok=True)
        else:
            folder = self.charts_dir
        
        filepath = folder / filename
        fig.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close(fig)
        
        rel_path = f"segment_charts/{subfolder}/{filename}" if subfolder else f"segment_charts/{filename}"
        self.generated_charts[rel_path] = str(filepath)
        print(f"✅ Saved: {rel_path}")
        return str(filepath)
    
    # ==================== DAILY METRICS CHARTS ====================
    
    def generate_dau_by_date_charts(self):
        """Generate charts for dau_by_date.csv"""
        df = self.load_csv(self.base_dir / "segments/daily/dau_by_date.csv")
        if df is None:
            return
        
        df['date'] = pd.to_datetime(df['date'])
        
        # 1. DAU Trend Over Time
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['date'], df['total_dau'], marker='o', linewidth=2, markersize=6)
        ax.set_title('DAU Trend Over Time', fontsize=16, fontweight='bold')
        ax.set_ylabel('Total DAU', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'dau_by_date_trend.png', 'daily')
        
        # 2. New vs Returning Users - Stacked Area
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.fill_between(df['date'], 0, df['new_users'], label='New Users', alpha=0.7)
        ax.fill_between(df['date'], df['new_users'], df['new_users'] + df['returning_users'], 
                        label='Returning Users', alpha=0.7)
        ax.set_title('New vs Returning Users (Stacked Area)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Users', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'dau_by_date_new_vs_returning.png', 'daily')
        
        # 3. User Growth Rate - Dual Y-axis
        fig, ax1 = plt.subplots(figsize=(12, 6))
        ax2 = ax1.twinx()
        line1 = ax1.plot(df['date'], df['new_user_percentage'], 'b-o', label='New User %', linewidth=2)
        line2 = ax2.plot(df['date'], df['returning_user_percentage'], 'r-s', label='Returning User %', linewidth=2)
        ax1.set_ylabel('New User Percentage', fontsize=12, color='b')
        ax2.set_ylabel('Returning User Percentage', fontsize=12, color='r')
        ax1.set_title('User Growth Rate', fontsize=16, fontweight='bold')
        ax1.set_xlabel('Date', fontsize=12)
        ax1.tick_params(axis='y', labelcolor='b')
        ax2.tick_params(axis='y', labelcolor='r')
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, loc='upper left')
        self.save_chart(fig, 'dau_by_date_growth_rate.png', 'daily')
        
        # 4. DAU vs Revenue Correlation
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(df['total_dau'], df['total_revenue'], alpha=0.6, s=100)
        ax.set_title('DAU vs Revenue Correlation', fontsize=16, fontweight='bold')
        ax.set_xlabel('Total DAU', fontsize=12)
        ax.set_ylabel('Total Revenue', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'dau_by_date_dau_vs_revenue.png', 'daily')
        
        # 5. Weekly Pattern Analysis
        df['day_of_week'] = df['date'].dt.day_name()
        df['day_of_week_num'] = df['date'].dt.dayofweek
        weekly_avg = df.groupby('day_of_week').agg({
            'total_dau': 'mean',
            'day_of_week_num': 'first'
        }).reset_index()
        weekly_avg = weekly_avg.sort_values('day_of_week_num')
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        weekly_avg = weekly_avg.set_index('day_of_week').reindex([d for d in day_order if d in weekly_avg.index])
        
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(range(len(weekly_avg)), weekly_avg['total_dau'], alpha=0.7)
        ax.set_title('Weekly Pattern Analysis - Average DAU by Day of Week', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average DAU', fontsize=12)
        ax.set_xlabel('Day of Week', fontsize=12)
        ax.set_xticks(range(len(weekly_avg)))
        ax.set_xticklabels(weekly_avg.index, rotation=45)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'dau_by_date_weekly_pattern.png', 'daily')
    
    def generate_dau_by_country_charts(self):
        """Generate charts for dau_by_country.csv"""
        df = self.load_csv(self.base_dir / "segments/daily/dau_by_country.csv")
        if df is None:
            return
        
        df['date'] = pd.to_datetime(df['date'])
        
        # 1. Top 10 Countries by DAU
        country_totals = df.groupby('country')['total_dau'].sum().nlargest(10).reset_index()
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(country_totals['country'], country_totals['total_dau'], alpha=0.7)
        ax.set_title('Top 10 Countries by DAU (Aggregated)', fontsize=16, fontweight='bold')
        ax.set_xlabel('Total DAU', fontsize=12)
        ax.set_ylabel('Country', fontsize=12)
        ax.grid(True, alpha=0.3, axis='x')
        self.save_chart(fig, 'dau_by_country_top10.png', 'daily')
        
        # 2. Country Growth Trends
        top_countries = df.groupby('country')['total_dau'].sum().nlargest(5).index.tolist()
        df_top = df[df['country'].isin(top_countries)]
        fig, ax = plt.subplots(figsize=(12, 6))
        for country in top_countries:
            country_data = df_top[df_top['country'] == country].sort_values('date')
            ax.plot(country_data['date'], country_data['total_dau'], marker='o', label=country, linewidth=2)
        ax.set_title('Country Growth Trends (Top 5 Countries)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Total DAU', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'dau_by_country_growth_trends.png', 'daily')
        
        # 3. New User Percentage by Country
        country_avg_new = df.groupby('country')['new_user_percentage'].mean().reset_index()
        country_avg_new = country_avg_new.sort_values('new_user_percentage', ascending=False).head(15)
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.bar(range(len(country_avg_new)), country_avg_new['new_user_percentage'], alpha=0.7)
        ax.set_title('New User Percentage by Country (Top 15)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average New User %', fontsize=12)
        ax.set_xlabel('Country', fontsize=12)
        ax.set_xticks(range(len(country_avg_new)))
        ax.set_xticklabels(country_avg_new['country'], rotation=45, ha='right')
        ax.grid(True, alpha=0.3, axis='y')
        self.save_chart(fig, 'dau_by_country_new_user_pct.png', 'daily')
        
        # 4. Country Revenue vs DAU
        country_agg = df.groupby('country').agg({
            'total_dau': 'sum',
            'total_revenue': 'sum'
        }).reset_index()
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(country_agg['total_dau'], country_agg['total_revenue'], 
                  s=100, alpha=0.6, c=range(len(country_agg)), cmap='viridis')
        ax.set_title('Country Revenue vs DAU', fontsize=16, fontweight='bold')
        ax.set_xlabel('Total DAU', fontsize=12)
        ax.set_ylabel('Total Revenue', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'dau_by_country_revenue_vs_dau.png', 'daily')
        
        # 5. Top Countries Timeline - Stacked Area
        top_10_countries = df.groupby('country')['total_dau'].sum().nlargest(10).index.tolist()
        df_top10 = df[df['country'].isin(top_10_countries)].sort_values('date')
        pivot = df_top10.pivot_table(index='date', columns='country', values='total_dau', aggfunc='sum').fillna(0)
        pivot = pivot.sort_index()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.stackplot(pivot.index, *[pivot[col] for col in pivot.columns], 
                     labels=pivot.columns, alpha=0.7)
        ax.set_title('Top 10 Countries Timeline (Stacked Area)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Total DAU', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'dau_by_country_timeline_stacked.png', 'daily')
    
    def generate_revenue_by_date_charts(self):
        """Generate charts for revenue_by_date.csv"""
        df = self.load_csv(self.base_dir / "segments/daily/revenue_by_date.csv")
        if df is None:
            return
        
        df['date'] = pd.to_datetime(df['date'])
        
        # 1. Total Revenue Trend
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['date'], df['total_revenue'], marker='o', linewidth=2, markersize=6, color='green')
        ax.set_title('Total Revenue Trend', fontsize=16, fontweight='bold')
        ax.set_ylabel('Revenue ($)', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'revenue_by_date_trend.png', 'daily')
        
        # 2. Revenue by Type Stacked
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.fill_between(df['date'], 0, df['iap_revenue'], label='IAP Revenue', alpha=0.7)
        ax.fill_between(df['date'], df['iap_revenue'], df['iap_revenue'] + df['ad_revenue'], 
                        label='Ad Revenue', alpha=0.7)
        ax.fill_between(df['date'], df['iap_revenue'] + df['ad_revenue'], 
                        df['iap_revenue'] + df['ad_revenue'] + df['subscription_revenue'],
                        label='Subscription Revenue', alpha=0.7)
        ax.set_title('Revenue by Type (Stacked Area)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Revenue ($)', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'revenue_by_date_by_type_stacked.png', 'daily')
        
        # 3. ARPU Trend
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['date'], df['avg_revenue_per_user'], marker='o', linewidth=2, markersize=6, color='purple')
        ax.set_title('ARPU Trend', fontsize=16, fontweight='bold')
        ax.set_ylabel('ARPU ($)', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'revenue_by_date_arpu_trend.png', 'daily')
        
        # 4. Revenue vs Revenue Users
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(df['revenue_users'], df['total_revenue'], alpha=0.6, s=100)
        ax.set_title('Revenue vs Revenue Users', fontsize=16, fontweight='bold')
        ax.set_xlabel('Revenue Users', fontsize=12)
        ax.set_ylabel('Total Revenue', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'revenue_by_date_revenue_vs_users.png', 'daily')
        
        # 5. Revenue Stream Comparison
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['date'], df['iap_revenue'], label='IAP Revenue', marker='o', linewidth=2)
        ax.plot(df['date'], df['ad_revenue'], label='Ad Revenue', marker='s', linewidth=2)
        ax.plot(df['date'], df['subscription_revenue'], label='Subscription Revenue', marker='^', linewidth=2)
        ax.set_title('Revenue Stream Comparison', fontsize=16, fontweight='bold')
        ax.set_ylabel('Revenue ($)', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'revenue_by_date_stream_comparison.png', 'daily')
    
    def generate_revenue_by_country_charts(self):
        """Generate charts for revenue_by_country.csv"""
        df = self.load_csv(self.base_dir / "segments/daily/revenue_by_country.csv")
        if df is None:
            return
        
        df['date'] = pd.to_datetime(df['date'])
        
        # 1. Top 10 Revenue Countries
        country_totals = df.groupby('country')['total_revenue'].sum().nlargest(10).reset_index()
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(country_totals['country'], country_totals['total_revenue'], alpha=0.7)
        ax.set_title('Top 10 Revenue Countries (Aggregated)', fontsize=16, fontweight='bold')
        ax.set_xlabel('Total Revenue ($)', fontsize=12)
        ax.set_ylabel('Country', fontsize=12)
        ax.grid(True, alpha=0.3, axis='x')
        self.save_chart(fig, 'revenue_by_country_top10.png', 'daily')
        
        # 2. Country Revenue Trend Lines
        top_countries = df.groupby('country')['total_revenue'].sum().nlargest(5).index.tolist()
        df_top = df[df['country'].isin(top_countries)].sort_values('date')
        fig, ax = plt.subplots(figsize=(12, 6))
        for country in top_countries:
            country_data = df_top[df_top['country'] == country]
            ax.plot(country_data['date'], country_data['total_revenue'], marker='o', label=country, linewidth=2)
        ax.set_title('Country Revenue Trend Lines (Top 5 Countries)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Total Revenue ($)', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'revenue_by_country_trend_lines.png', 'daily')
        
        # 3. ARPU by Country
        country_arpu = df.groupby('country')['avg_revenue_per_user'].mean().reset_index()
        country_arpu = country_arpu.sort_values('avg_revenue_per_user', ascending=False).head(15)
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.bar(range(len(country_arpu)), country_arpu['avg_revenue_per_user'], alpha=0.7)
        ax.set_title('ARPU by Country (Top 15)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average ARPU ($)', fontsize=12)
        ax.set_xlabel('Country', fontsize=12)
        ax.set_xticks(range(len(country_arpu)))
        ax.set_xticklabels(country_arpu['country'], rotation=45, ha='right')
        ax.grid(True, alpha=0.3, axis='y')
        self.save_chart(fig, 'revenue_by_country_arpu.png', 'daily')
        
        # 4. Revenue Mix by Country
        top_10_countries = df.groupby('country')['total_revenue'].sum().nlargest(10).index.tolist()
        df_top10 = df[df['country'].isin(top_10_countries)]
        country_revenue_mix = df_top10.groupby('country').agg({
            'iap_revenue': 'sum',
            'ad_revenue': 'sum',
            'subscription_revenue': 'sum'
        }).reset_index()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        x = np.arange(len(country_revenue_mix))
        width = 0.25
        ax.bar(x - width, country_revenue_mix['iap_revenue'], width, label='IAP Revenue', alpha=0.7)
        ax.bar(x, country_revenue_mix['ad_revenue'], width, label='Ad Revenue', alpha=0.7)
        ax.bar(x + width, country_revenue_mix['subscription_revenue'], width, label='Subscription Revenue', alpha=0.7)
        ax.set_title('Revenue Mix by Country (Top 10)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Revenue ($)', fontsize=12)
        ax.set_xlabel('Country', fontsize=12)
        ax.set_xticks(x)
        ax.set_xticklabels(country_revenue_mix['country'], rotation=45, ha='right')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        self.save_chart(fig, 'revenue_by_country_mix.png', 'daily')
        
        # 5. Country Revenue vs Users
        country_agg = df.groupby('country').agg({
            'revenue_users': 'sum',
            'total_revenue': 'sum'
        }).reset_index()
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(country_agg['revenue_users'], country_agg['total_revenue'], 
                  s=100, alpha=0.6, c=range(len(country_agg)), cmap='viridis')
        ax.set_title('Country Revenue vs Users', fontsize=16, fontweight='bold')
        ax.set_xlabel('Revenue Users', fontsize=12)
        ax.set_ylabel('Total Revenue ($)', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'revenue_by_country_revenue_vs_users.png', 'daily')
        
        # 6. Top Countries Revenue Timeline - Stacked Area
        top_10_countries = df.groupby('country')['total_revenue'].sum().nlargest(10).index.tolist()
        df_top10 = df[df['country'].isin(top_10_countries)].sort_values('date')
        pivot = df_top10.pivot_table(index='date', columns='country', values='total_revenue', aggfunc='sum').fillna(0)
        pivot = pivot.sort_index()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.stackplot(pivot.index, *[pivot[col] for col in pivot.columns], 
                     labels=pivot.columns, alpha=0.7)
        ax.set_title('Top 10 Countries Revenue Timeline (Stacked Area)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Total Revenue ($)', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'revenue_by_country_timeline_stacked.png', 'daily')
        
        # 7. IAP vs Ad Revenue by Country
        top_10_countries = df.groupby('country')['total_revenue'].sum().nlargest(10).index.tolist()
        df_top10 = df[df['country'].isin(top_10_countries)]
        country_revenue = df_top10.groupby('country').agg({
            'iap_revenue': 'sum',
            'ad_revenue': 'sum'
        }).reset_index()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        x = np.arange(len(country_revenue))
        width = 0.35
        ax.bar(x - width/2, country_revenue['iap_revenue'], width, label='IAP Revenue', alpha=0.7)
        ax.bar(x + width/2, country_revenue['ad_revenue'], width, label='Ad Revenue', alpha=0.7)
        ax.set_title('IAP vs Ad Revenue by Country (Top 10)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Revenue ($)', fontsize=12)
        ax.set_xlabel('Country', fontsize=12)
        ax.set_xticks(x)
        ax.set_xticklabels(country_revenue['country'], rotation=45, ha='right')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        self.save_chart(fig, 'revenue_by_country_iap_vs_ad.png', 'daily')
    
    def generate_revenue_by_type_charts(self):
        """Generate charts for revenue_by_type.csv"""
        df = self.load_csv(self.base_dir / "segments/daily/revenue_by_type.csv")
        if df is None:
            return
        
        df['date'] = pd.to_datetime(df['date'])
        
        # 1. Revenue by Segment Over Time
        segments = df['revenue_segment'].unique()
        fig, ax = plt.subplots(figsize=(12, 6))
        for segment in segments:
            segment_data = df[df['revenue_segment'] == segment].sort_values('date')
            ax.plot(segment_data['date'], segment_data['total_revenue'], marker='o', label=segment, linewidth=2)
        ax.set_title('Revenue by Segment Over Time', fontsize=16, fontweight='bold')
        ax.set_ylabel('Total Revenue ($)', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'revenue_by_type_segment_trend.png', 'daily')
        
        # 2. Segment Revenue Stacked
        df_sorted = df.sort_values('date')
        pivot = df_sorted.pivot_table(index='date', columns='revenue_segment', values='total_revenue', aggfunc='sum').fillna(0)
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.stackplot(pivot.index, *[pivot[col] for col in pivot.columns], 
                     labels=pivot.columns, alpha=0.7)
        ax.set_title('Segment Revenue (Stacked Area)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Total Revenue ($)', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'revenue_by_type_segment_stacked.png', 'daily')
        
        # 3. ARPU by Segment
        segment_arpu = df.groupby('revenue_segment')['avg_revenue_per_user'].mean().reset_index()
        segment_arpu = segment_arpu.sort_values('avg_revenue_per_user', ascending=False)
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(segment_arpu['revenue_segment'], segment_arpu['avg_revenue_per_user'], alpha=0.7)
        ax.set_title('ARPU by Segment', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average ARPU ($)', fontsize=12)
        ax.set_xlabel('Revenue Segment', fontsize=12)
        ax.grid(True, alpha=0.3, axis='y')
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'revenue_by_type_arpu_by_segment.png', 'daily')
        
        # 4. Segment User Count vs Revenue
        segment_agg = df.groupby('revenue_segment').agg({
            'revenue_users': 'sum',
            'total_revenue': 'sum'
        }).reset_index()
        fig, ax = plt.subplots(figsize=(10, 6))
        colors = plt.cm.viridis(np.linspace(0, 1, len(segment_agg)))
        for i, (_, row) in enumerate(segment_agg.iterrows()):
            ax.scatter(row['revenue_users'], row['total_revenue'], 
                     s=200, alpha=0.6, c=[colors[i]], label=row['revenue_segment'])
        ax.set_title('Segment User Count vs Revenue', fontsize=16, fontweight='bold')
        ax.set_xlabel('Revenue Users', fontsize=12)
        ax.set_ylabel('Total Revenue ($)', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'revenue_by_type_users_vs_revenue.png', 'daily')
    
    def generate_new_logins_by_country_charts(self):
        """Generate charts for new_logins_by_country.csv"""
        df = self.load_csv(self.base_dir / "segments/daily/new_logins_by_country.csv")
        if df is None:
            return
        
        df['date'] = pd.to_datetime(df['date'])
        
        # 1. New Logins Trend by Top Countries
        top_countries = df.groupby('country')['new_logins'].sum().nlargest(10).index.tolist()
        df_top = df[df['country'].isin(top_countries)].sort_values('date')
        fig, ax = plt.subplots(figsize=(12, 6))
        for country in top_countries:
            country_data = df_top[df_top['country'] == country]
            ax.plot(country_data['date'], country_data['new_logins'], marker='o', label=country, linewidth=2)
        ax.set_title('New Logins Trend by Top Countries', fontsize=16, fontweight='bold')
        ax.set_ylabel('New Logins', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'new_logins_by_country_trend.png', 'daily')
        
        # 2. New User Revenue Heatmap
        pivot = df.pivot_table(index='date', columns='country', values='new_user_revenue', aggfunc='sum').fillna(0)
        top_15_countries = df.groupby('country')['new_user_revenue'].sum().nlargest(15).index.tolist()
        pivot = pivot[top_15_countries]
        
        fig, ax = plt.subplots(figsize=(14, 8))
        sns.heatmap(pivot.T, annot=False, fmt='.0f', cmap='YlOrRd', ax=ax, cbar_kws={'label': 'Revenue ($)'})
        ax.set_title('New User Revenue Heatmap (Date vs Country)', fontsize=16, fontweight='bold')
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Country', fontsize=12)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'new_logins_by_country_heatmap.png', 'daily')
        
        # 3. New User ARPU by Country
        country_arpu = df.groupby('country')['avg_revenue_per_new_user'].mean().reset_index()
        country_arpu = country_arpu.sort_values('avg_revenue_per_new_user', ascending=False).head(15)
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.bar(range(len(country_arpu)), country_arpu['avg_revenue_per_new_user'], alpha=0.7)
        ax.set_title('New User ARPU by Country (Top 15)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average ARPU ($)', fontsize=12)
        ax.set_xlabel('Country', fontsize=12)
        ax.set_xticks(range(len(country_arpu)))
        ax.set_xticklabels(country_arpu['country'], rotation=45, ha='right')
        ax.grid(True, alpha=0.3, axis='y')
        self.save_chart(fig, 'new_logins_by_country_arpu.png', 'daily')
        
        # 4. Acquisition vs Revenue Correlation
        country_agg = df.groupby('country').agg({
            'new_logins': 'sum',
            'new_user_revenue': 'sum'
        }).reset_index()
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(country_agg['new_logins'], country_agg['new_user_revenue'], 
                  s=100, alpha=0.6, c=range(len(country_agg)), cmap='viridis')
        ax.set_title('Acquisition vs Revenue Correlation', fontsize=16, fontweight='bold')
        ax.set_xlabel('New Logins', fontsize=12)
        ax.set_ylabel('New User Revenue ($)', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'new_logins_by_country_acquisition_vs_revenue.png', 'daily')
        
        # 5. Top Countries Acquisition Timeline
        top_10_countries = df.groupby('country')['new_logins'].sum().nlargest(10).index.tolist()
        df_top10 = df[df['country'].isin(top_10_countries)].sort_values('date')
        pivot = df_top10.pivot_table(index='date', columns='country', values='new_logins', aggfunc='sum').fillna(0)
        pivot = pivot.sort_index()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.stackplot(pivot.index, *[pivot[col] for col in pivot.columns], 
                     labels=pivot.columns, alpha=0.7)
        ax.set_title('Top Countries Acquisition Timeline (Stacked Area)', fontsize=16, fontweight='bold')
        ax.set_ylabel('New Logins', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'new_logins_by_country_timeline_stacked.png', 'daily')
    
    def generate_engagement_by_date_charts(self):
        """Generate charts for engagement_by_date.csv"""
        df = self.load_csv(self.base_dir / "segments/daily/engagement_by_date.csv")
        if df is None:
            return
        
        df['date'] = pd.to_datetime(df['date'])
        
        # 1. Engagement Score Trend
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['date'], df['avg_engagement_score'], marker='o', linewidth=2, markersize=6)
        ax.set_title('Engagement Score Trend', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average Engagement Score', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'engagement_by_date_score_trend.png', 'daily')
        
        # 2. Session Time Trend
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['date'], df['avg_session_time'], marker='o', linewidth=2, markersize=6, color='orange')
        ax.set_title('Session Time Trend', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average Session Time', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'engagement_by_date_session_time.png', 'daily')
        
        # 3. Events per User Trend
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['date'], df['avg_events'], marker='o', linewidth=2, markersize=6, color='green')
        ax.set_title('Events per User Trend', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average Events', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'engagement_by_date_events_trend.png', 'daily')
        
        # 4. Engagement vs Users
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(df['total_users'], df['avg_engagement_score'], alpha=0.6, s=100)
        ax.set_title('Engagement vs Users', fontsize=16, fontweight='bold')
        ax.set_xlabel('Total Users', fontsize=12)
        ax.set_ylabel('Average Engagement Score', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'engagement_by_date_engagement_vs_users.png', 'daily')
        
        # 5. Engagement Components - Normalized
        fig, ax = plt.subplots(figsize=(12, 6))
        # Normalize each metric to 0-1 scale for comparison
        df_norm = df.copy()
        for col in ['avg_engagement_score', 'avg_session_time', 'avg_events']:
            df_norm[col + '_norm'] = (df_norm[col] - df_norm[col].min()) / (df_norm[col].max() - df_norm[col].min())
        ax.plot(df_norm['date'], df_norm['avg_engagement_score_norm'], label='Engagement Score', marker='o', linewidth=2)
        ax.plot(df_norm['date'], df_norm['avg_session_time_norm'], label='Session Time', marker='s', linewidth=2)
        ax.plot(df_norm['date'], df_norm['avg_events_norm'], label='Events', marker='^', linewidth=2)
        ax.set_title('Engagement Components (Normalized)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Normalized Value (0-1)', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'engagement_by_date_components.png', 'daily')
        
        # 6. Engagement Distribution
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.hist(df['avg_engagement_score'], bins=20, alpha=0.7, edgecolor='black')
        ax.set_title('Engagement Score Distribution', fontsize=16, fontweight='bold')
        ax.set_xlabel('Average Engagement Score', fontsize=12)
        ax.set_ylabel('Frequency', fontsize=12)
        ax.grid(True, alpha=0.3, axis='y')
        self.save_chart(fig, 'engagement_by_date_distribution.png', 'daily')
        
        # 7. Weekly Engagement Pattern
        df['day_of_week'] = df['date'].dt.day_name()
        df['day_of_week_num'] = df['date'].dt.dayofweek
        weekly_avg = df.groupby('day_of_week').agg({
            'avg_engagement_score': 'mean',
            'day_of_week_num': 'first'
        }).reset_index()
        weekly_avg = weekly_avg.sort_values('day_of_week_num')
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        weekly_avg = weekly_avg.set_index('day_of_week').reindex([d for d in day_order if d in weekly_avg.index])
        
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(range(len(weekly_avg)), weekly_avg['avg_engagement_score'], alpha=0.7)
        ax.set_title('Weekly Engagement Pattern - Average by Day of Week', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average Engagement Score', fontsize=12)
        ax.set_xlabel('Day of Week', fontsize=12)
        ax.set_xticks(range(len(weekly_avg)))
        ax.set_xticklabels(weekly_avg.index, rotation=45)
        ax.grid(True, alpha=0.3, axis='y')
        self.save_chart(fig, 'engagement_by_date_weekly_pattern.png', 'daily')
    
    # ==================== COHORT METRICS CHARTS ====================
    
    def generate_dau_by_cohort_date_charts(self):
        """Generate charts for dau_by_cohort_date.csv"""
        df = self.load_csv(self.base_dir / "segments/cohort/dau_by_cohort_date.csv")
        if df is None:
            return
        
        df['cohort_date'] = pd.to_datetime(df['cohort_date'])
        
        # 1. Cohort DAU Retention Curve
        fig, ax = plt.subplots(figsize=(12, 6))
        days = ['day_0_dau', 'day_1_dau', 'day_3_dau', 'day_7_dau', 'day_14_dau', 'day_30_dau', 'day_60_dau']
        day_labels = [0, 1, 3, 7, 14, 30, 60]
        
        # Plot last 10 cohorts
        recent_cohorts = df.sort_values('cohort_date').tail(10)
        for idx, row in recent_cohorts.iterrows():
            values = [row[day] for day in days]
            ax.plot(day_labels, values, marker='o', label=row['cohort_date'].strftime('%m/%d'), linewidth=2, alpha=0.7)
        ax.set_title('Cohort DAU Retention Curve (Last 10 Cohorts)', fontsize=16, fontweight='bold')
        ax.set_ylabel('DAU', fontsize=12)
        ax.set_xlabel('Days Since Cohort', fontsize=12)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'dau_by_cohort_date_retention_curve.png', 'cohort')
        
        # 2. Cohort Size Trend
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['cohort_date'], df['cohort_size'], marker='o', linewidth=2, markersize=6)
        ax.set_title('Cohort Size Trend', fontsize=16, fontweight='bold')
        ax.set_ylabel('Cohort Size', fontsize=12)
        ax.set_xlabel('Cohort Date', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'dau_by_cohort_date_size_trend.png', 'cohort')
        
        # 3. Cohort Comparison - Average DAU
        days = ['day_0_dau', 'day_1_dau', 'day_3_dau', 'day_7_dau', 'day_14_dau', 'day_30_dau', 'day_60_dau']
        day_labels = [0, 1, 3, 7, 14, 30, 60]
        avg_dau = [df[day].mean() for day in days]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(day_labels, avg_dau, marker='o', linewidth=2, markersize=8)
        ax.set_title('Cohort Comparison - Average DAU Across Cohorts', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average DAU', fontsize=12)
        ax.set_xlabel('Days Since Cohort', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'dau_by_cohort_date_comparison.png', 'cohort')
        
        # 4. Cohort Size vs DAU Correlation
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(df['cohort_size'], df['day_0_dau'], alpha=0.6, s=100)
        ax.set_title('Cohort Size vs Day 0 DAU Correlation', fontsize=16, fontweight='bold')
        ax.set_xlabel('Cohort Size', fontsize=12)
        ax.set_ylabel('Day 0 DAU', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'dau_by_cohort_date_size_vs_dau.png', 'cohort')
        
        # 5. Top Cohorts by Size
        top_cohorts = df.nlargest(10, 'cohort_size')
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.barh(top_cohorts['cohort_date'].dt.strftime('%m/%d'), top_cohorts['cohort_size'], alpha=0.7)
        ax.set_title('Top 10 Cohorts by Size', fontsize=16, fontweight='bold')
        ax.set_xlabel('Cohort Size', fontsize=12)
        ax.set_ylabel('Cohort Date', fontsize=12)
        ax.grid(True, alpha=0.3, axis='x')
        self.save_chart(fig, 'dau_by_cohort_date_top_cohorts.png', 'cohort')
    
    def generate_retention_by_cohort_date_charts(self):
        """Generate charts for retention_by_cohort_date.csv"""
        df = self.load_csv(self.base_dir / "segments/cohort/retention_by_cohort_date.csv")
        if df is None:
            return
        
        df['cohort_date'] = pd.to_datetime(df['cohort_date'])
        
        # 1. Retention Curve by Cohort
        retention_days = ['day_1_retention_rate', 'day_3_retention_rate', 'day_7_retention_rate', 
                         'day_14_retention_rate', 'day_30_retention_rate']
        day_labels = [1, 3, 7, 14, 30]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        recent_cohorts = df.sort_values('cohort_date').tail(10)
        for idx, row in recent_cohorts.iterrows():
            values = [row[day] for day in retention_days]
            ax.plot(day_labels, values, marker='o', label=row['cohort_date'].strftime('%m/%d'), linewidth=2, alpha=0.7)
        ax.set_title('Retention Curve by Cohort (Last 10 Cohorts)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Retention Rate (%)', fontsize=12)
        ax.set_xlabel('Days Since Cohort', fontsize=12)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'retention_by_cohort_date_curve.png', 'cohort')
        
        # 2. Retention Heatmap
        retention_days = ['day_1_retention_rate', 'day_3_retention_rate', 'day_7_retention_rate', 
                         'day_14_retention_rate', 'day_30_retention_rate']
        day_labels_short = ['D1', 'D3', 'D7', 'D14', 'D30']
        
        recent_cohorts = df.sort_values('cohort_date').tail(20)
        heatmap_data = []
        for idx, row in recent_cohorts.iterrows():
            heatmap_data.append([row[day] for day in retention_days])
        
        heatmap_df = pd.DataFrame(heatmap_data, 
                                  index=recent_cohorts['cohort_date'].dt.strftime('%m/%d'),
                                  columns=day_labels_short)
        
        fig, ax = plt.subplots(figsize=(10, 8))
        sns.heatmap(heatmap_df, annot=True, fmt='.1f', cmap='YlGnBu', ax=ax, cbar_kws={'label': 'Retention Rate (%)'})
        ax.set_title('Retention Heatmap (Cohort Date vs Retention Day)', fontsize=16, fontweight='bold')
        ax.set_xlabel('Retention Day', fontsize=12)
        ax.set_ylabel('Cohort Date', fontsize=12)
        self.save_chart(fig, 'retention_by_cohort_date_heatmap.png', 'cohort')
        
        # 3. Cohort Retention Comparison
        retention_days = ['day_1_retention_rate', 'day_3_retention_rate', 'day_7_retention_rate', 
                         'day_14_retention_rate', 'day_30_retention_rate']
        day_labels = [1, 3, 7, 14, 30]
        avg_retention = [df[day].mean() for day in retention_days]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(day_labels, avg_retention, marker='o', linewidth=2, markersize=8)
        ax.set_title('Cohort Retention Comparison - Average Retention Rate', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average Retention Rate (%)', fontsize=12)
        ax.set_xlabel('Days Since Cohort', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'retention_by_cohort_date_comparison.png', 'cohort')
        
        # 4. D1 Retention Trend
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['cohort_date'], df['day_1_retention_rate'], marker='o', linewidth=2, markersize=6)
        ax.set_title('D1 Retention Trend', fontsize=16, fontweight='bold')
        ax.set_ylabel('D1 Retention Rate (%)', fontsize=12)
        ax.set_xlabel('Cohort Date', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'retention_by_cohort_date_d1_trend.png', 'cohort')
        
        # 5. D7 Retention Trend
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['cohort_date'], df['day_7_retention_rate'], marker='o', linewidth=2, markersize=6, color='orange')
        ax.set_title('D7 Retention Trend', fontsize=16, fontweight='bold')
        ax.set_ylabel('D7 Retention Rate (%)', fontsize=12)
        ax.set_xlabel('Cohort Date', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'retention_by_cohort_date_d7_trend.png', 'cohort')
        
        # 6. Cohort Size vs Retention
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(df['cohort_size'], df['day_1_retention_rate'], alpha=0.6, s=100)
        ax.set_title('Cohort Size vs D1 Retention', fontsize=16, fontweight='bold')
        ax.set_xlabel('Cohort Size', fontsize=12)
        ax.set_ylabel('D1 Retention Rate (%)', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'retention_by_cohort_date_size_vs_retention.png', 'cohort')
    
    def generate_revenue_by_cohort_date_charts(self):
        """Generate charts for revenue_by_cohort_date.csv"""
        df = self.load_csv(self.base_dir / "segments/cohort/revenue_by_cohort_date.csv")
        if df is None:
            return
        
        df['cohort_date'] = pd.to_datetime(df['cohort_date'])
        
        # 1. Cohort Revenue Curve
        revenue_days = ['day_0_revenue', 'day_1_revenue', 'day_3_revenue', 'day_7_revenue', 
                       'day_14_revenue', 'day_30_revenue', 'day_60_revenue']
        day_labels = [0, 1, 3, 7, 14, 30, 60]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        recent_cohorts = df.sort_values('cohort_date').tail(10)
        for idx, row in recent_cohorts.iterrows():
            values = [row[day] for day in revenue_days]
            ax.plot(day_labels, values, marker='o', label=row['cohort_date'].strftime('%m/%d'), linewidth=2, alpha=0.7)
        ax.set_title('Cohort Revenue Curve (Last 10 Cohorts)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Revenue ($)', fontsize=12)
        ax.set_xlabel('Days Since Cohort', fontsize=12)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'revenue_by_cohort_date_curve.png', 'cohort')
        
        # 2. Total Cohort Revenue
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.bar(range(len(df)), df['total_cohort_revenue'], alpha=0.7)
        ax.set_title('Total Cohort Revenue by Cohort Date', fontsize=16, fontweight='bold')
        ax.set_ylabel('Total Revenue ($)', fontsize=12)
        ax.set_xlabel('Cohort Index', fontsize=12)
        ax.set_xticks(range(len(df)))
        ax.set_xticklabels(df['cohort_date'].dt.strftime('%m/%d'), rotation=45)
        ax.grid(True, alpha=0.3, axis='y')
        self.save_chart(fig, 'revenue_by_cohort_date_total.png', 'cohort')
        
        # 3. Cohort Revenue Comparison
        revenue_days = ['day_0_revenue', 'day_1_revenue', 'day_3_revenue', 'day_7_revenue', 
                       'day_14_revenue', 'day_30_revenue', 'day_60_revenue']
        day_labels = [0, 1, 3, 7, 14, 30, 60]
        avg_revenue = [df[day].mean() for day in revenue_days]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(day_labels, avg_revenue, marker='o', linewidth=2, markersize=8)
        ax.set_title('Cohort Revenue Comparison - Average Revenue', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average Revenue ($)', fontsize=12)
        ax.set_xlabel('Days Since Cohort', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'revenue_by_cohort_date_comparison.png', 'cohort')
        
        # 4. Cohort Size vs Revenue
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(df['cohort_size'], df['total_cohort_revenue'], alpha=0.6, s=100)
        ax.set_title('Cohort Size vs Total Revenue', fontsize=16, fontweight='bold')
        ax.set_xlabel('Cohort Size', fontsize=12)
        ax.set_ylabel('Total Revenue ($)', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'revenue_by_cohort_date_size_vs_revenue.png', 'cohort')
        
        # 5. Top Revenue Cohorts
        top_cohorts = df.nlargest(10, 'total_cohort_revenue')
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.barh(top_cohorts['cohort_date'].dt.strftime('%m/%d'), top_cohorts['total_cohort_revenue'], alpha=0.7)
        ax.set_title('Top 10 Revenue Cohorts', fontsize=16, fontweight='bold')
        ax.set_xlabel('Total Revenue ($)', fontsize=12)
        ax.set_ylabel('Cohort Date', fontsize=12)
        ax.grid(True, alpha=0.3, axis='x')
        self.save_chart(fig, 'revenue_by_cohort_date_top_cohorts.png', 'cohort')
    
    def generate_engagement_by_cohort_date_charts(self):
        """Generate charts for engagement_by_cohort_date.csv"""
        df = self.load_csv(self.base_dir / "segments/cohort/engagement_by_cohort_date.csv")
        if df is None:
            return
        
        df['cohort_date'] = pd.to_datetime(df['cohort_date'])
        
        # 1. Engagement Score by Cohort Age
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['cohort_date'], df['avg_engagement_score_day_0'], marker='o', label='Day 0', linewidth=2)
        if 'avg_engagement_score_day_1' in df.columns:
            ax.plot(df['cohort_date'], df['avg_engagement_score_day_1'], marker='s', label='Day 1', linewidth=2)
        if 'avg_engagement_score_day_7' in df.columns:
            ax.plot(df['cohort_date'], df['avg_engagement_score_day_7'], marker='^', label='Day 7', linewidth=2)
        ax.set_title('Engagement Score by Cohort Age', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average Engagement Score', fontsize=12)
        ax.set_xlabel('Cohort Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'engagement_by_cohort_date_score.png', 'cohort')
        
        # 2. Session Count by Cohort Age
        fig, ax = plt.subplots(figsize=(12, 6))
        if 'avg_sessions_day_0' in df.columns:
            ax.plot(df['cohort_date'], df['avg_sessions_day_0'], marker='o', label='Day 0', linewidth=2)
        if 'avg_sessions_day_1' in df.columns:
            ax.plot(df['cohort_date'], df['avg_sessions_day_1'], marker='s', label='Day 1', linewidth=2)
        if 'avg_sessions_day_7' in df.columns:
            ax.plot(df['cohort_date'], df['avg_sessions_day_7'], marker='^', label='Day 7', linewidth=2)
        ax.set_title('Session Count by Cohort Age', fontsize=16, fontweight='bold')
        ax.set_ylabel('Average Sessions', fontsize=12)
        ax.set_xlabel('Cohort Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'engagement_by_cohort_date_sessions.png', 'cohort')
        
        # 3. Engagement vs Sessions Correlation
        if 'avg_engagement_score_day_0' in df.columns and 'avg_sessions_day_0' in df.columns:
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.scatter(df['avg_sessions_day_0'], df['avg_engagement_score_day_0'], alpha=0.6, s=100)
            ax.set_title('Engagement vs Sessions Correlation (Day 0)', fontsize=16, fontweight='bold')
            ax.set_xlabel('Average Sessions', fontsize=12)
            ax.set_ylabel('Average Engagement Score', fontsize=12)
            ax.grid(True, alpha=0.3)
            self.save_chart(fig, 'engagement_by_cohort_date_correlation.png', 'cohort')
        
        # 4. Top Engaged Cohorts
        top_cohorts = df.nlargest(10, 'avg_engagement_score_day_0')
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.barh(top_cohorts['cohort_date'].dt.strftime('%m/%d'), top_cohorts['avg_engagement_score_day_0'], alpha=0.7)
        ax.set_title('Top 10 Engaged Cohorts (Day 0)', fontsize=16, fontweight='bold')
        ax.set_xlabel('Average Engagement Score', fontsize=12)
        ax.set_ylabel('Cohort Date', fontsize=12)
        ax.grid(True, alpha=0.3, axis='x')
        self.save_chart(fig, 'engagement_by_cohort_date_top_cohorts.png', 'cohort')
    
    def generate_revenue_by_cohort_country_charts(self):
        """Generate charts for revenue_by_cohort_country.csv"""
        df = self.load_csv(self.base_dir / "segments/cohort/revenue_by_cohort_country.csv")
        if df is None:
            return
        
        df['cohort_date'] = pd.to_datetime(df['cohort_date'])
        
        # Extract country columns (they follow pattern {country}_revenue and {country}_users)
        country_cols = [col for col in df.columns if col.endswith('_revenue') and col != 'total_cohort_revenue']
        countries = [col.replace('_revenue', '') for col in country_cols]
        
        if not countries:
            print("⚠️  No country revenue columns found")
            return
        
        # 1. Top Countries by Cohort Revenue
        country_totals = {}
        for country in countries:
            revenue_col = f"{country}_revenue"
            if revenue_col in df.columns:
                country_totals[country] = df[revenue_col].sum()
        
        top_countries_df = pd.DataFrame(list(country_totals.items()), columns=['country', 'total_revenue'])
        top_countries_df = top_countries_df.sort_values('total_revenue', ascending=False).head(15)
        
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(top_countries_df['country'], top_countries_df['total_revenue'], alpha=0.7)
        ax.set_title('Top Countries by Cohort Revenue (Aggregated)', fontsize=16, fontweight='bold')
        ax.set_xlabel('Total Revenue ($)', fontsize=12)
        ax.set_ylabel('Country', fontsize=12)
        ax.grid(True, alpha=0.3, axis='x')
        self.save_chart(fig, 'revenue_by_cohort_country_top_countries.png', 'cohort')
        
        # 2. Country Revenue Trends by Cohort
        top_5_countries = top_countries_df.head(5)['country'].tolist()
        fig, ax = plt.subplots(figsize=(12, 6))
        for country in top_5_countries:
            revenue_col = f"{country}_revenue"
            if revenue_col in df.columns:
                country_data = df.groupby('cohort_date')[revenue_col].sum().reset_index().sort_values('cohort_date')
                ax.plot(country_data['cohort_date'], country_data[revenue_col], marker='o', label=country, linewidth=2)
        ax.set_title('Country Revenue Trends by Cohort (Top 5 Countries)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Revenue ($)', fontsize=12)
        ax.set_xlabel('Cohort Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'revenue_by_cohort_country_trends.png', 'cohort')
        
        # 3. Country Revenue Contribution - Pie Chart
        fig, ax = plt.subplots(figsize=(10, 8))
        top_15_countries_df = top_countries_df.head(15)
        ax.pie(top_15_countries_df['total_revenue'], labels=top_15_countries_df['country'], 
               autopct='%1.1f%%', startangle=90)
        ax.set_title('Country Revenue Contribution (Top 15 Countries)', fontsize=16, fontweight='bold')
        self.save_chart(fig, 'revenue_by_cohort_country_contribution.png', 'cohort')
    
    def generate_funnel_by_cohort_date_charts(self):
        """Generate charts for funnel_by_cohort_date.csv"""
        df = self.load_csv(self.base_dir / "segments/cohort/funnel_by_cohort_date.csv")
        if df is None:
            return
        
        df['cohort_date'] = pd.to_datetime(df['cohort_date'])
        
        # 1. Funnel Conversion by Cohort
        fig, ax = plt.subplots(figsize=(12, 6))
        if 'ftue_completion_rate' in df.columns:
            ax.plot(df['cohort_date'], df['ftue_completion_rate'], marker='o', label='FTUE Completion', linewidth=2)
        if 'level_1_completion_rate' in df.columns:
            ax.plot(df['cohort_date'], df['level_1_completion_rate'], marker='s', label='Level 1 Completion', linewidth=2)
        if 'purchase_rate' in df.columns:
            ax.plot(df['cohort_date'], df['purchase_rate'], marker='^', label='Purchase Rate', linewidth=2)
        ax.set_title('Funnel Conversion by Cohort', fontsize=16, fontweight='bold')
        ax.set_ylabel('Conversion Rate (%)', fontsize=12)
        ax.set_xlabel('Cohort Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'funnel_by_cohort_date_conversion.png', 'cohort')
        
        # 2. Funnel Heatmap
        funnel_stages = []
        if 'ftue_completion_rate' in df.columns:
            funnel_stages.append(('FTUE Complete', 'ftue_completion_rate'))
        if 'level_1_completion_rate' in df.columns:
            funnel_stages.append(('Level 1', 'level_1_completion_rate'))
        if 'purchase_rate' in df.columns:
            funnel_stages.append(('Purchase', 'purchase_rate'))
        
        if funnel_stages:
            recent_cohorts = df.sort_values('cohort_date').tail(20)
            heatmap_data = []
            for idx, row in recent_cohorts.iterrows():
                heatmap_data.append([row[stage_col] for _, stage_col in funnel_stages])
            
            heatmap_df = pd.DataFrame(heatmap_data,
                                      index=recent_cohorts['cohort_date'].dt.strftime('%m/%d'),
                                      columns=[stage_name for stage_name, _ in funnel_stages])
            
            fig, ax = plt.subplots(figsize=(8, 10))
            sns.heatmap(heatmap_df, annot=True, fmt='.1f', cmap='YlGnBu', ax=ax, cbar_kws={'label': 'Rate (%)'})
            ax.set_title('Funnel Heatmap (Cohort Date vs Funnel Stage)', fontsize=16, fontweight='bold')
            ax.set_xlabel('Funnel Stage', fontsize=12)
            ax.set_ylabel('Cohort Date', fontsize=12)
            self.save_chart(fig, 'funnel_by_cohort_date_heatmap.png', 'cohort')
    
    def generate_event_funnel_by_cohort_date_charts(self):
        """Generate charts for event_funnel_by_cohort_date.csv"""
        df = self.load_csv(self.base_dir / "segments/cohort/event_funnel_by_cohort_date.csv")
        if df is None:
            return
        
        df['cohort_date'] = pd.to_datetime(df['cohort_date'])
        
        # Extract level columns (they follow pattern level_{N}_rate)
        level_cols = [col for col in df.columns if col.startswith('level_') and col.endswith('_rate')]
        levels = sorted([col.replace('_rate', '') for col in level_cols], 
                        key=lambda x: int(x.split('_')[1]) if x.split('_')[1].isdigit() else 0)
        
        if not levels:
            print("⚠️  No level rate columns found")
            return
        
        # 1. Level Progression by Cohort
        fig, ax = plt.subplots(figsize=(12, 6))
        recent_cohorts = df.sort_values('cohort_date').tail(10)
        for level in levels[:7]:  # Limit to first 7 levels
            level_col = f"{level}_rate"
            if level_col in df.columns:
                level_data = recent_cohorts.groupby('cohort_date')[level_col].mean().reset_index().sort_values('cohort_date')
                ax.plot(level_data['cohort_date'], level_data[level_col], marker='o', label=level, linewidth=2)
        ax.set_title('Level Progression by Cohort (Last 10 Cohorts)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Completion Rate (%)', fontsize=12)
        ax.set_xlabel('Cohort Date', fontsize=12)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'event_funnel_by_cohort_date_progression.png', 'cohort')
        
        # 2. Level Drop-off Analysis
        level_rates = []
        level_nums = []
        for level in levels:
            level_col = f"{level}_rate"
            if level_col in df.columns:
                avg_rate = df[level_col].mean()
                level_rates.append(avg_rate)
                level_num = int(level.split('_')[1]) if level.split('_')[1].isdigit() else 0
                level_nums.append(level_num)
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(level_nums, level_rates, marker='o', linewidth=2, markersize=8)
        ax.set_title('Level Drop-off Analysis - Average Completion Rate', fontsize=16, fontweight='bold')
        ax.set_ylabel('Completion Rate (%)', fontsize=12)
        ax.set_xlabel('Level Number', fontsize=12)
        ax.grid(True, alpha=0.3)
        self.save_chart(fig, 'event_funnel_by_cohort_date_dropoff.png', 'cohort')
        
        # 3. Top Performing Cohorts by Level
        if 'level_7_rate' in df.columns:
            top_cohorts = df.nlargest(10, 'level_7_rate')
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.barh(top_cohorts['cohort_date'].dt.strftime('%m/%d'), top_cohorts['level_7_rate'], alpha=0.7)
            ax.set_title('Top 10 Cohorts by Level 7 Completion Rate', fontsize=16, fontweight='bold')
            ax.set_xlabel('Level 7 Completion Rate (%)', fontsize=12)
            ax.set_ylabel('Cohort Date', fontsize=12)
            ax.grid(True, alpha=0.3, axis='x')
            self.save_chart(fig, 'event_funnel_by_cohort_date_top_cohorts.png', 'cohort')
    
    def generate_user_journey_cohort_charts(self):
        """Generate charts for user_journey_cohort.csv"""
        df = self.load_csv(self.base_dir / "segments/user_level/user_journey_cohort.csv")
        if df is None:
            return
        
        if 'stage_completion_date' in df.columns:
            df['stage_completion_date'] = pd.to_datetime(df['stage_completion_date'])
        if 'cohort_date' in df.columns:
            df['cohort_date'] = pd.to_datetime(df['cohort_date'])
        
        # 1. Stage Completion Timeline
        if 'stage_completion_date' in df.columns and 'journey_stage' in df.columns:
            stage_counts = df.groupby(['stage_completion_date', 'journey_stage']).size().reset_index(name='user_count')
            pivot = stage_counts.pivot(index='stage_completion_date', columns='journey_stage', values='user_count').fillna(0)
            pivot = pivot.sort_index()
            
            fig, ax = plt.subplots(figsize=(12, 6))
            for stage in pivot.columns:
                stage_data = pivot[stage]
                if stage_data.sum() > 0:  # Only plot if there's data
                    ax.plot(pivot.index, stage_data, marker='o', label=stage, linewidth=2, alpha=0.7)
            ax.set_title('Stage Completion Timeline', fontsize=16, fontweight='bold')
            ax.set_ylabel('User Count', fontsize=12)
            ax.set_xlabel('Stage Completion Date', fontsize=12)
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
            ax.grid(True, alpha=0.3)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            self.save_chart(fig, 'user_journey_cohort_timeline.png', 'user_level')
        
        # 2. Time to Stage by Cohort
        if 'time_to_stage_days' in df.columns and 'cohort_date' in df.columns and 'journey_stage' in df.columns:
            # Get top 5 stages
            top_stages = df['journey_stage'].value_counts().head(5).index.tolist()
            df_top = df[df['journey_stage'].isin(top_stages)]
            
            # Limit to recent cohorts for readability
            recent_cohorts = sorted(df_top['cohort_date'].unique())[-10:]
            df_top = df_top[df_top['cohort_date'].isin(recent_cohorts)]
            
            if len(df_top) > 0:
                fig, axes = plt.subplots(1, len(top_stages), figsize=(16, 5))
                if len(top_stages) == 1:
                    axes = [axes]
                
                for idx, stage in enumerate(top_stages):
                    stage_data = df_top[df_top['journey_stage'] == stage]
                    if len(stage_data) > 0:
                        # Group by cohort_date and create boxplot data
                        cohort_groups = []
                        cohort_labels = []
                        for cohort in recent_cohorts:
                            cohort_values = stage_data[stage_data['cohort_date'] == cohort]['time_to_stage_days'].dropna()
                            if len(cohort_values) > 0:
                                cohort_groups.append(cohort_values.values)
                                cohort_labels.append(cohort.strftime('%m/%d'))
                        
                        if cohort_groups:
                            axes[idx].boxplot(cohort_groups, labels=cohort_labels)
                            axes[idx].set_title(f'{stage}', fontsize=12, fontweight='bold')
                            axes[idx].set_xlabel('Cohort Date', fontsize=10)
                            axes[idx].set_ylabel('Time to Stage (Days)', fontsize=10)
                            axes[idx].tick_params(axis='x', rotation=45, labelsize=8)
                
                plt.suptitle('Time to Stage by Cohort (Box Plot)', fontsize=16, fontweight='bold', y=1.02)
                plt.tight_layout()
                self.save_chart(plt.gcf(), 'user_journey_cohort_time_to_stage.png', 'user_level')
        
        # 3. Journey Progression Rate
        if 'journey_stage' in df.columns:
            stage_counts = df['journey_stage'].value_counts().sort_index()
            total_users = len(df['user_id'].unique())
            progression_rates = (stage_counts / total_users * 100) if total_users > 0 else stage_counts * 0
            
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.bar(range(len(progression_rates)), progression_rates.values, alpha=0.7)
            ax.set_title('Journey Progression Rate', fontsize=16, fontweight='bold')
            ax.set_ylabel('Completion Rate (%)', fontsize=12)
            ax.set_xlabel('Journey Stage', fontsize=12)
            ax.set_xticks(range(len(progression_rates)))
            ax.set_xticklabels(progression_rates.index, rotation=45, ha='right')
            ax.grid(True, alpha=0.3, axis='y')
            self.save_chart(fig, 'user_journey_cohort_progression_rate.png', 'user_level')
    
    # ==================== USER LEVEL METRICS CHARTS ====================
    
    def generate_behavioral_segments_daily_charts(self):
        """Generate charts for behavioral_segments_daily.csv"""
        df = self.load_csv(self.base_dir / "segments/user_level/behavioral_segments_daily.csv")
        if df is None:
            return
        
        df['date'] = pd.to_datetime(df['date'])
        
        # 1. Segment Distribution Over Time
        segment_counts = df.groupby(['date', 'behavioral_segment']).size().reset_index(name='user_count')
        pivot = segment_counts.pivot(index='date', columns='behavioral_segment', values='user_count').fillna(0)
        pivot = pivot.sort_index()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.stackplot(pivot.index, *[pivot[col] for col in pivot.columns], 
                     labels=pivot.columns, alpha=0.7)
        ax.set_title('Segment Distribution Over Time (Stacked Area)', fontsize=16, fontweight='bold')
        ax.set_ylabel('User Count', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'behavioral_segments_daily_distribution.png', 'user_level')
        
        # 2. Segment Distribution Pie
        segment_totals = df['behavioral_segment'].value_counts()
        fig, ax = plt.subplots(figsize=(10, 8))
        ax.pie(segment_totals.values, labels=segment_totals.index, autopct='%1.1f%%', startangle=90)
        ax.set_title('Segment Distribution (Pie Chart)', fontsize=16, fontweight='bold')
        self.save_chart(fig, 'behavioral_segments_daily_pie.png', 'user_level')
        
        # 3. Engagement Score Distribution
        fig, ax = plt.subplots(figsize=(12, 6))
        segments = df['behavioral_segment'].unique()
        for segment in segments:
            segment_data = df[df['behavioral_segment'] == segment]['engagement_score']
            ax.hist(segment_data, bins=20, alpha=0.5, label=segment, edgecolor='black')
        ax.set_title('Engagement Score Distribution by Segment', fontsize=16, fontweight='bold')
        ax.set_xlabel('Engagement Score', fontsize=12)
        ax.set_ylabel('Frequency', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        self.save_chart(fig, 'behavioral_segments_daily_engagement_dist.png', 'user_level')
        
        # 4. Segment Trend Lines
        segment_counts = df.groupby(['date', 'behavioral_segment']).size().reset_index(name='user_count')
        pivot = segment_counts.pivot(index='date', columns='behavioral_segment', values='user_count').fillna(0)
        pivot = pivot.sort_index()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        for segment in pivot.columns:
            ax.plot(pivot.index, pivot[segment], marker='o', label=segment, linewidth=2)
        ax.set_title('Segment Trend Lines', fontsize=16, fontweight='bold')
        ax.set_ylabel('User Count', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'behavioral_segments_daily_trends.png', 'user_level')
    
    def generate_revenue_segments_daily_charts(self):
        """Generate charts for revenue_segments_daily.csv"""
        df = self.load_csv(self.base_dir / "segments/user_level/revenue_segments_daily.csv")
        if df is None:
            return
        
        df['date'] = pd.to_datetime(df['date'])
        
        # 1. Revenue Segment Distribution - Stacked Area
        segment_counts = df.groupby(['date', 'revenue_segment']).size().reset_index(name='user_count')
        pivot = segment_counts.pivot(index='date', columns='revenue_segment', values='user_count').fillna(0)
        pivot = pivot.sort_index()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.stackplot(pivot.index, *[pivot[col] for col in pivot.columns], 
                     labels=pivot.columns, alpha=0.7)
        ax.set_title('Revenue Segment Distribution Over Time (Stacked Area)', fontsize=16, fontweight='bold')
        ax.set_ylabel('User Count', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'revenue_segments_daily_distribution.png', 'user_level')
        
        # 2. Revenue Segment Pie
        segment_totals = df['revenue_segment'].value_counts()
        fig, ax = plt.subplots(figsize=(10, 8))
        ax.pie(segment_totals.values, labels=segment_totals.index, autopct='%1.1f%%', startangle=90)
        ax.set_title('Revenue Segment Distribution (Pie Chart)', fontsize=16, fontweight='bold')
        self.save_chart(fig, 'revenue_segments_daily_pie.png', 'user_level')
        
        # 3. Segment Revenue Trends
        segment_revenue = df.groupby(['date', 'revenue_segment'])['total_revenue'].sum().reset_index()
        pivot = segment_revenue.pivot(index='date', columns='revenue_segment', values='total_revenue').fillna(0)
        pivot = pivot.sort_index()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        for segment in pivot.columns:
            ax.plot(pivot.index, pivot[segment], marker='o', label=segment, linewidth=2)
        ax.set_title('Segment Revenue Trends', fontsize=16, fontweight='bold')
        ax.set_ylabel('Total Revenue ($)', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.save_chart(fig, 'revenue_segments_daily_revenue_trends.png', 'user_level')
        
        # 4. Revenue Mix by Segment
        segment_revenue_mix = df.groupby('revenue_segment').agg({
            'iap_revenue': 'sum',
            'ad_revenue': 'sum'
        }).reset_index()
        
        fig, ax = plt.subplots(figsize=(10, 6))
        x = np.arange(len(segment_revenue_mix))
        width = 0.35
        ax.bar(x - width/2, segment_revenue_mix['iap_revenue'], width, label='IAP Revenue', alpha=0.7)
        ax.bar(x + width/2, segment_revenue_mix['ad_revenue'], width, label='Ad Revenue', alpha=0.7)
        ax.set_title('Revenue Mix by Segment', fontsize=16, fontweight='bold')
        ax.set_ylabel('Revenue ($)', fontsize=12)
        ax.set_xlabel('Revenue Segment', fontsize=12)
        ax.set_xticks(x)
        ax.set_xticklabels(segment_revenue_mix['revenue_segment'], rotation=45)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        self.save_chart(fig, 'revenue_segments_daily_revenue_mix.png', 'user_level')
    
    # ==================== MAIN GENERATION METHOD ====================
    
    def generate_all_charts(self):
        """Generate all charts for all segment files."""
        print("🚀 Starting comprehensive chart generation...")
        print(f"📁 Output directory: {self.charts_dir}\n")
        
        # Daily Metrics
        print("📊 Generating Daily Metrics Charts...")
        self.generate_dau_by_date_charts()
        self.generate_dau_by_country_charts()
        self.generate_revenue_by_date_charts()
        self.generate_revenue_by_country_charts()
        self.generate_revenue_by_type_charts()
        self.generate_new_logins_by_country_charts()
        self.generate_engagement_by_date_charts()
        
        # Cohort Metrics
        print("\n📊 Generating Cohort Metrics Charts...")
        self.generate_dau_by_cohort_date_charts()
        self.generate_retention_by_cohort_date_charts()
        self.generate_revenue_by_cohort_date_charts()
        self.generate_engagement_by_cohort_date_charts()
        self.generate_revenue_by_cohort_country_charts()
        self.generate_funnel_by_cohort_date_charts()
        self.generate_event_funnel_by_cohort_date_charts()
        
        # User Level Metrics
        print("\n📊 Generating User Level Metrics Charts...")
        self.generate_behavioral_segments_daily_charts()
        self.generate_revenue_segments_daily_charts()
        self.generate_user_journey_cohort_charts()
        
        print(f"\n✅ Chart generation complete!")
        print(f"📊 Generated {len(self.generated_charts)} charts")
        print(f"📁 Charts saved in: {self.charts_dir}")
        
        return self.generated_charts


def main():
    parser = argparse.ArgumentParser(description='Generate comprehensive charts for segment output files')
    parser.add_argument('--run-hash', required=True, help='Run hash (e.g., 1388d6)')
    args = parser.parse_args()
    
    generator = ComprehensiveChartGenerator(args.run_hash)
    generator.generate_all_charts()


if __name__ == "__main__":
    main()


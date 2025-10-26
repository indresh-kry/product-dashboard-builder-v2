"""
Report Visualization Generator
Version: 1.0.0
Last Updated: 2025-10-24

This module generates visualizations for the agentic insights reports.
Creates charts for DAU trends, revenue trends, retention funnels, and event funnels.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime
from typing import Dict, Any, List, Optional
import json

# Set style for better-looking charts
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class ReportVisualizationGenerator:
    """Generates visualizations for agentic insights reports."""
    
    def __init__(self, run_hash: str):
        self.run_hash = run_hash
        self.output_dir = f"run_logs/{run_hash}/outputs/insights/visualizations"
        self.data_dir = f"run_logs/{run_hash}/outputs"
        
        # Create visualization directory
        os.makedirs(self.output_dir, exist_ok=True)
        
    def generate_all_charts(self) -> Dict[str, str]:
        """Generate all charts and return file paths."""
        charts = {}
        
        try:
            # 1. DAU Trends
            charts['dau_trend'] = self.create_dau_trend_chart()
            
            # 2. Revenue Trends
            charts['revenue_trend'] = self.create_revenue_trend_chart()
            
            # 3. Retention Funnels
            charts['retention_funnel'] = self.create_retention_funnel_chart()
            
            # 4. Event Funnels
            charts['event_funnel'] = self.create_event_funnel_chart()
            
            print(f"✅ Generated {len(charts)} charts successfully")
            return charts
            
        except Exception as e:
            print(f"❌ Error generating charts: {e}")
            return {}
    
    def create_dau_trend_chart(self) -> str:
        """Create DAU trend chart at daily level."""
        try:
            # Load DAU data
            dau_file = f"{self.data_dir}/segments/daily/dau_by_date.csv"
            if not os.path.exists(dau_file):
                print(f"⚠️ DAU data file not found: {dau_file}")
                return ""
                
            df = pd.read_csv(dau_file)
            df['date'] = pd.to_datetime(df['date'])
            
            # Create figure
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
            
            # DAU Trend
            ax1.plot(df['date'], df['total_dau'], marker='o', linewidth=2, markersize=6)
            ax1.set_title('Daily Active Users Trend', fontsize=16, fontweight='bold')
            ax1.set_ylabel('DAU', fontsize=12)
            ax1.grid(True, alpha=0.3)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # New vs Returning Users
            ax2.plot(df['date'], df['new_users'], label='New Users', marker='o', linewidth=2)
            ax2.plot(df['date'], df['returning_users'], label='Returning Users', marker='s', linewidth=2)
            ax2.set_title('New vs Returning Users Trend', fontsize=16, fontweight='bold')
            ax2.set_ylabel('Users', fontsize=12)
            ax2.set_xlabel('Date', fontsize=12)
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax2.xaxis.set_major_locator(mdates.DayLocator(interval=2))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            plt.tight_layout()
            
            # Save chart
            chart_path = f"{self.output_dir}/dau_trend.png"
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"✅ DAU trend chart saved: {chart_path}")
            return chart_path
            
        except Exception as e:
            print(f"❌ Error creating DAU trend chart: {e}")
            return ""
    
    def create_revenue_trend_chart(self) -> str:
        """Create revenue trend chart at daily level."""
        try:
            # Load revenue data
            revenue_file = f"{self.data_dir}/segments/daily/revenue_by_date.csv"
            if not os.path.exists(revenue_file):
                print(f"⚠️ Revenue data file not found: {revenue_file}")
                return ""
                
            df = pd.read_csv(revenue_file)
            df['date'] = pd.to_datetime(df['date'])
            
            # Create figure
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
            
            # Total Revenue Trend
            ax1.plot(df['date'], df['total_revenue'], marker='o', linewidth=2, markersize=6, color='green')
            ax1.set_title('Daily Revenue Trend', fontsize=16, fontweight='bold')
            ax1.set_ylabel('Revenue ($)', fontsize=12)
            ax1.grid(True, alpha=0.3)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # Revenue by Type
            ax2.plot(df['date'], df['iap_revenue'], label='IAP Revenue', marker='o', linewidth=2)
            ax2.plot(df['date'], df['ad_revenue'], label='Ad Revenue', marker='s', linewidth=2)
            ax2.plot(df['date'], df['subscription_revenue'], label='Subscription Revenue', marker='^', linewidth=2)
            ax2.set_title('Revenue by Type Trend', fontsize=16, fontweight='bold')
            ax2.set_ylabel('Revenue ($)', fontsize=12)
            ax2.set_xlabel('Date', fontsize=12)
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax2.xaxis.set_major_locator(mdates.DayLocator(interval=2))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            plt.tight_layout()
            
            # Save chart
            chart_path = f"{self.output_dir}/revenue_trend.png"
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"✅ Revenue trend chart saved: {chart_path}")
            return chart_path
            
        except Exception as e:
            print(f"❌ Error creating revenue trend chart: {e}")
            return ""
    
    def create_retention_funnel_chart(self) -> str:
        """Create retention funnel chart for D1, D3, D7 at cohort level."""
        try:
            # Load aggregated data for cohort analysis
            agg_file = f"{self.data_dir}/aggregations/aggregated_data.csv"
            if not os.path.exists(agg_file):
                print(f"⚠️ Aggregated data file not found: {agg_file}")
                return ""
                
            df = pd.read_csv(agg_file)
            df['date'] = pd.to_datetime(df['date'])
            df['cohort_date'] = pd.to_datetime(df['cohort_date'])
            
            # Calculate retention rates by cohort
            retention_data = []
            
            # Get unique cohort dates
            cohort_dates = sorted(df['cohort_date'].dropna().unique())
            
            for cohort_date in cohort_dates[-10:]:  # Last 10 cohorts
                cohort_users = df[df['cohort_date'] == cohort_date]
                
                if len(cohort_users) == 0:
                    continue
                    
                # Calculate D1, D3, D7 retention
                d1_retained = len(df[(df['cohort_date'] == cohort_date) & (df['days_since_first_event'] == 1)])
                d3_retained = len(df[(df['cohort_date'] == cohort_date) & (df['days_since_first_event'] == 3)])
                d7_retained = len(df[(df['cohort_date'] == cohort_date) & (df['days_since_first_event'] == 7)])
                
                total_cohort = len(cohort_users)
                
                if total_cohort > 0:
                    retention_data.append({
                        'cohort_date': cohort_date.strftime('%m/%d'),
                        'total_users': total_cohort,
                        'd1_retention': (d1_retained / total_cohort) * 100,
                        'd3_retention': (d3_retained / total_cohort) * 100,
                        'd7_retention': (d7_retained / total_cohort) * 100
                    })
            
            if not retention_data:
                print("⚠️ No retention data available")
                return ""
                
            retention_df = pd.DataFrame(retention_data)
            
            # Create funnel chart
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
            
            # Retention Rates by Cohort
            x_pos = range(len(retention_df))
            width = 0.25
            
            ax1.bar([x - width for x in x_pos], retention_df['d1_retention'], width, label='D1 Retention', alpha=0.8)
            ax1.bar(x_pos, retention_df['d3_retention'], width, label='D3 Retention', alpha=0.8)
            ax1.bar([x + width for x in x_pos], retention_df['d7_retention'], width, label='D7 Retention', alpha=0.8)
            
            ax1.set_title('Retention Rates by Cohort', fontsize=16, fontweight='bold')
            ax1.set_ylabel('Retention Rate (%)', fontsize=12)
            ax1.set_xlabel('Cohort Date', fontsize=12)
            ax1.set_xticks(x_pos)
            ax1.set_xticklabels(retention_df['cohort_date'], rotation=45)
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            
            # Average Retention Funnel
            avg_d1 = retention_df['d1_retention'].mean()
            avg_d3 = retention_df['d3_retention'].mean()
            avg_d7 = retention_df['d7_retention'].mean()
            
            funnel_data = [avg_d1, avg_d3, avg_d7]
            funnel_labels = ['D1 Retention', 'D3 Retention', 'D7 Retention']
            
            bars = ax2.bar(funnel_labels, funnel_data, color=['#ff9999', '#66b3ff', '#99ff99'], alpha=0.8)
            ax2.set_title('Average Retention Funnel', fontsize=16, fontweight='bold')
            ax2.set_ylabel('Retention Rate (%)', fontsize=12)
            ax2.grid(True, alpha=0.3)
            
            # Add value labels on bars
            for bar, value in zip(bars, funnel_data):
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                        f'{value:.1f}%', ha='center', va='bottom', fontweight='bold')
            
            plt.tight_layout()
            
            # Save chart
            chart_path = f"{self.output_dir}/retention_funnel.png"
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"✅ Retention funnel chart saved: {chart_path}")
            return chart_path
            
        except Exception as e:
            print(f"❌ Error creating retention funnel chart: {e}")
            return ""
    
    def create_event_funnel_chart(self) -> str:
        """Create event funnel chart for app opened, first level completed, etc."""
        try:
            # Load aggregated data for event analysis
            agg_file = f"{self.data_dir}/aggregations/aggregated_data.csv"
            if not os.path.exists(agg_file):
                print(f"⚠️ Aggregated data file not found: {agg_file}")
                return ""
                
            df = pd.read_csv(agg_file)
            
            # Calculate event funnel metrics
            total_users = len(df)
            
            # App opened (users with any events)
            app_opened = len(df[df['total_events'] > 0])
            
            # First level completed (users with level_1_time)
            first_level_completed = len(df[df['level_1_time'].notna()])
            
            # Second level completed (users with level_2_time)
            second_level_completed = len(df[df['level_2_time'].notna()])
            
            # Third level completed (users with level_3_time)
            third_level_completed = len(df[df['level_3_time'].notna()])
            
            # FTUE completed (users with ftue_complete_time)
            ftue_completed = len(df[df['ftue_complete_time'].notna()])
            
            # Game completed (users with game_complete_time)
            game_completed = len(df[df['game_complete_time'].notna()])
            
            # Create funnel data
            funnel_data = [
                total_users,
                app_opened,
                first_level_completed,
                second_level_completed,
                third_level_completed,
                ftue_completed,
                game_completed
            ]
            
            funnel_labels = [
                'Total Users',
                'App Opened',
                'Level 1 Completed',
                'Level 2 Completed',
                'Level 3 Completed',
                'FTUE Completed',
                'Game Completed'
            ]
            
            # Calculate conversion rates
            conversion_rates = []
            for i in range(1, len(funnel_data)):
                if funnel_data[i-1] > 0:
                    rate = (funnel_data[i] / funnel_data[i-1]) * 100
                    conversion_rates.append(rate)
                else:
                    conversion_rates.append(0)
            
            # Create funnel chart
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 8))
            
            # User Count Funnel
            colors = plt.cm.viridis(range(len(funnel_data)))
            bars = ax1.barh(range(len(funnel_data)), funnel_data, color=colors, alpha=0.8)
            ax1.set_yticks(range(len(funnel_data)))
            ax1.set_yticklabels(funnel_labels)
            ax1.set_title('Event Funnel - User Counts', fontsize=16, fontweight='bold')
            ax1.set_xlabel('Number of Users', fontsize=12)
            ax1.grid(True, alpha=0.3)
            
            # Add value labels
            for i, (bar, value) in enumerate(zip(bars, funnel_data)):
                width = bar.get_width()
                ax1.text(width + max(funnel_data) * 0.01, bar.get_y() + bar.get_height()/2,
                        f'{value:,}', ha='left', va='center', fontweight='bold')
            
            # Conversion Rates
            conversion_labels = funnel_labels[1:]  # Exclude 'Total Users'
            bars2 = ax2.bar(range(len(conversion_rates)), conversion_rates, 
                           color=plt.cm.viridis(range(len(conversion_rates))), alpha=0.8)
            ax2.set_title('Event Funnel - Conversion Rates', fontsize=16, fontweight='bold')
            ax2.set_ylabel('Conversion Rate (%)', fontsize=12)
            ax2.set_xticks(range(len(conversion_labels)))
            ax2.set_xticklabels(conversion_labels, rotation=45, ha='right')
            ax2.grid(True, alpha=0.3)
            
            # Add value labels
            for bar, value in zip(bars2, conversion_rates):
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                        f'{value:.1f}%', ha='center', va='bottom', fontweight='bold')
            
            plt.tight_layout()
            
            # Save chart
            chart_path = f"{self.output_dir}/event_funnel.png"
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"✅ Event funnel chart saved: {chart_path}")
            return chart_path
            
        except Exception as e:
            print(f"❌ Error creating event funnel chart: {e}")
            return ""

    def generate_chart_summary(self, charts: Dict[str, str]) -> str:
        """Generate a summary of generated charts."""
        summary = []
        summary.append("## 📊 Visual Analytics")
        summary.append("")
        
        if charts.get('dau_trend'):
            summary.append("### 📈 User Engagement Trends")
            summary.append("![DAU Trends](visualizations/dau_trend.png)")
            summary.append("")
            
        if charts.get('revenue_trend'):
            summary.append("### 💰 Revenue Performance")
            summary.append("![Revenue Trends](visualizations/revenue_trend.png)")
            summary.append("")
            
        if charts.get('retention_funnel'):
            summary.append("### 🔄 User Retention Analysis")
            summary.append("![Retention Funnel](visualizations/retention_funnel.png)")
            summary.append("")
            
        if charts.get('event_funnel'):
            summary.append("### 🎯 Event Conversion Funnel")
            summary.append("![Event Funnel](visualizations/event_funnel.png)")
            summary.append("")
        
        return "\n".join(summary)

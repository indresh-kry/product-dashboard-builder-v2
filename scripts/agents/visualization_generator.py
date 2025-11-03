"""
Report Visualization Generator
Version: 1.2.0
Last Updated: 2025-10-31

This module generates visualizations for the agentic insights reports.
Creates charts for DAU trends, revenue trends, retention funnels, event funnels, and ARPU performance.

Changelog:
- v1.2.0 (2025-10-31): Added new visualizations:
    - Daily user count by acquisition channel in User Engagement Trends
    - Daily revenue by acquisition channel and geography in Revenue Performance
    - New ARPU Performance section with overall ARPU and ARPU by acquisition channel
- v1.1.0 (2025-10-31): Added date filtering logic to exclude incomplete dates from all visualizations
    - Finds the last date present across all 4 data sources (DAU, Revenue, Aggregated)
    - Filters all visualizations to exclude dates after the last complete date
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
        
    def find_last_complete_date(self) -> Optional[str]:
        """Find the last date that has entries across all 4 visualization data sources.
        
        Returns:
            Last complete date (YYYY-MM-DD format) or None if data sources don't exist
        """
        try:
            date_sets = []
            
            # 1. DAU data dates
            dau_file = f"{self.data_dir}/segments/daily/dau_by_date.csv"
            if os.path.exists(dau_file):
                dau_df = pd.read_csv(dau_file)
                if 'date' in dau_df.columns:
                    dau_df['date'] = pd.to_datetime(dau_df['date'])
                    date_sets.append(set(dau_df['date'].dt.date.unique()))
                else:
                    return None
            else:
                return None
            
            # 2. Revenue data dates
            revenue_file = f"{self.data_dir}/segments/daily/revenue_by_date.csv"
            if os.path.exists(revenue_file):
                revenue_df = pd.read_csv(revenue_file)
                if 'date' in revenue_df.columns:
                    revenue_df['date'] = pd.to_datetime(revenue_df['date'])
                    date_sets.append(set(revenue_df['date'].dt.date.unique()))
                else:
                    return None
            else:
                return None
            
            # 3. Aggregated data dates (for retention and event funnels)
            agg_file = f"{self.data_dir}/aggregations/aggregated_data.csv"
            if os.path.exists(agg_file):
                agg_df = pd.read_csv(agg_file)
                if 'date' in agg_df.columns:
                    agg_df['date'] = pd.to_datetime(agg_df['date'])
                    date_sets.append(set(agg_df['date'].dt.date.unique()))
                else:
                    return None
            else:
                return None
            
            # Find intersection of all date sets (dates present in all 3 sources)
            # Note: Retention and Event funnels use the same aggregated_data.csv
            if len(date_sets) >= 3:
                common_dates = date_sets[0]
                for date_set in date_sets[1:]:
                    common_dates = common_dates.intersection(date_set)
                
                if common_dates:
                    last_complete_date = max(common_dates)
                    print(f"📅 Last complete date across all visualizations: {last_complete_date}")
                    return last_complete_date.strftime('%Y-%m-%d')
            
            return None
            
        except Exception as e:
            print(f"⚠️ Error finding last complete date: {e}")
            return None
    
    def generate_all_charts(self) -> Dict[str, str]:
        """Generate all charts and return file paths."""
        charts = {}
        
        try:
            # Find last complete date across all data sources
            last_complete_date = self.find_last_complete_date()
            
            if last_complete_date:
                print(f"📊 Filtering all visualizations to dates up to {last_complete_date}")
                self.last_complete_date = pd.to_datetime(last_complete_date)
            else:
                print("⚠️ Could not determine last complete date, generating charts without date filtering")
                self.last_complete_date = None
            
            # 1. DAU Trends
            charts['dau_trend'] = self.create_dau_trend_chart()
            
            # 2. Revenue Trends
            charts['revenue_trend'] = self.create_revenue_trend_chart()
            
            # 3. ARPU Performance
            charts['arpu_performance'] = self.create_arpu_performance_chart()
            
            # 4. Retention Funnels
            charts['retention_funnel'] = self.create_retention_funnel_chart()
            
            # 5. Event Funnels
            charts['event_funnel'] = self.create_event_funnel_chart()
            
            print(f"✅ Generated {len(charts)} charts successfully")
            return charts
            
        except Exception as e:
            print(f"❌ Error generating charts: {e}")
            return {}
    
    def create_dau_trend_chart(self) -> str:
        """Create DAU trend chart at daily level with acquisition channel breakdown."""
        try:
            # Load DAU data
            dau_file = f"{self.data_dir}/segments/daily/dau_by_date.csv"
            if not os.path.exists(dau_file):
                print(f"⚠️ DAU data file not found: {dau_file}")
                return ""
                
            df_dau = pd.read_csv(dau_file)
            df_dau['date'] = pd.to_datetime(df_dau['date'])
            
            # Filter to last complete date if set
            if hasattr(self, 'last_complete_date') and self.last_complete_date is not None:
                df_dau = df_dau[df_dau['date'] <= self.last_complete_date]
            
            # Load aggregated data for acquisition channel breakdown
            agg_file = f"{self.data_dir}/aggregations/aggregated_data.csv"
            df_agg = None
            if os.path.exists(agg_file):
                df_agg = pd.read_csv(agg_file)
                if 'date' in df_agg.columns:
                    df_agg['date'] = pd.to_datetime(df_agg['date'])
                    if hasattr(self, 'last_complete_date') and self.last_complete_date is not None:
                        df_agg = df_agg[df_agg['date'] <= self.last_complete_date]
            
            # Create figure with 3 subplots
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 14))
            
            # DAU Trend
            ax1.plot(df_dau['date'], df_dau['total_dau'], marker='o', linewidth=2, markersize=6)
            ax1.set_title('Daily Active Users Trend', fontsize=16, fontweight='bold')
            ax1.set_ylabel('DAU', fontsize=12)
            ax1.grid(True, alpha=0.3)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # New vs Returning Users
            ax2.plot(df_dau['date'], df_dau['new_users'], label='New Users', marker='o', linewidth=2)
            ax2.plot(df_dau['date'], df_dau['returning_users'], label='Returning Users', marker='s', linewidth=2)
            ax2.set_title('New vs Returning Users Trend', fontsize=16, fontweight='bold')
            ax2.set_ylabel('Users', fontsize=12)
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax2.xaxis.set_major_locator(mdates.DayLocator(interval=2))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # Daily User Count by Acquisition Channel (using media_source from aggregate table)
            if df_agg is not None and 'media_source' in df_agg.columns:
                # Calculate daily unique users by media_source
                daily_users_by_channel = df_agg.groupby(['date', 'media_source'])['user_id'].nunique().reset_index()
                daily_users_by_channel.columns = ['date', 'media_source', 'user_count']
                
                # Get top channels (limit to top 10 to avoid clutter)
                top_channels = daily_users_by_channel.groupby('media_source')['user_count'].sum().nlargest(10).index.tolist()
                daily_users_by_channel = daily_users_by_channel[daily_users_by_channel['media_source'].isin(top_channels)]
                
                # Pivot for plotting
                pivot_df = daily_users_by_channel.pivot(index='date', columns='media_source', values='user_count').fillna(0)
                pivot_df = pivot_df.sort_index()
                
                # Plot each channel
                for channel in pivot_df.columns:
                    ax3.plot(pivot_df.index, pivot_df[channel], label=channel, marker='o', linewidth=2, markersize=4)
                
                ax3.set_title('Daily User Count by Acquisition Channel', fontsize=16, fontweight='bold')
                ax3.set_ylabel('User Count', fontsize=12)
                ax3.set_xlabel('Date', fontsize=12)
                ax3.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
                ax3.grid(True, alpha=0.3)
                ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                ax3.xaxis.set_major_locator(mdates.DayLocator(interval=2))
                plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
            else:
                ax3.text(0.5, 0.5, 'Acquisition channel data not available', 
                        ha='center', va='center', transform=ax3.transAxes, fontsize=12)
                ax3.set_title('Daily User Count by Acquisition Channel', fontsize=16, fontweight='bold')
            
            plt.tight_layout()
            
            # Save chart
            chart_path = f"{self.output_dir}/dau_trend.png"
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"✅ DAU trend chart saved: {chart_path}")
            return chart_path
            
        except Exception as e:
            print(f"❌ Error creating DAU trend chart: {e}")
            import traceback
            traceback.print_exc()
            return ""
    
    def create_revenue_trend_chart(self) -> str:
        """Create revenue trend chart at daily level with acquisition channel and geography breakdown."""
        try:
            # Load revenue data
            revenue_file = f"{self.data_dir}/segments/daily/revenue_by_date.csv"
            if not os.path.exists(revenue_file):
                print(f"⚠️ Revenue data file not found: {revenue_file}")
                return ""
                
            df_revenue = pd.read_csv(revenue_file)
            df_revenue['date'] = pd.to_datetime(df_revenue['date'])
            
            # Filter to last complete date if set
            if hasattr(self, 'last_complete_date') and self.last_complete_date is not None:
                df_revenue = df_revenue[df_revenue['date'] <= self.last_complete_date]
            
            # Load aggregated data for channel and geography breakdown
            agg_file = f"{self.data_dir}/aggregations/aggregated_data.csv"
            df_agg = None
            if os.path.exists(agg_file):
                df_agg = pd.read_csv(agg_file)
                if 'date' in df_agg.columns:
                    df_agg['date'] = pd.to_datetime(df_agg['date'])
                    if hasattr(self, 'last_complete_date') and self.last_complete_date is not None:
                        df_agg = df_agg[df_agg['date'] <= self.last_complete_date]
            
            # Create figure with 4 subplots
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
            
            # Total Revenue Trend
            ax1.plot(df_revenue['date'], df_revenue['total_revenue'], marker='o', linewidth=2, markersize=6, color='green')
            ax1.set_title('Daily Revenue Trend', fontsize=16, fontweight='bold')
            ax1.set_ylabel('Revenue ($)', fontsize=12)
            ax1.grid(True, alpha=0.3)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # Revenue by Type
            ax2.plot(df_revenue['date'], df_revenue['iap_revenue'], label='IAP Revenue', marker='o', linewidth=2)
            ax2.plot(df_revenue['date'], df_revenue['ad_revenue'], label='Ad Revenue', marker='s', linewidth=2)
            ax2.plot(df_revenue['date'], df_revenue['subscription_revenue'], label='Subscription Revenue', marker='^', linewidth=2)
            ax2.set_title('Revenue by Type Trend', fontsize=16, fontweight='bold')
            ax2.set_ylabel('Revenue ($)', fontsize=12)
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax2.xaxis.set_major_locator(mdates.DayLocator(interval=2))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # Daily Revenue by Acquisition Channel (using media_source from aggregate table)
            if df_agg is not None and 'media_source' in df_agg.columns and 'total_revenue' in df_agg.columns:
                # Calculate daily total revenue by media_source
                daily_revenue_by_channel = df_agg.groupby(['date', 'media_source'])['total_revenue'].sum().reset_index()
                
                # Get top channels (limit to top 10 to avoid clutter)
                top_channels = daily_revenue_by_channel.groupby('media_source')['total_revenue'].sum().nlargest(10).index.tolist()
                daily_revenue_by_channel = daily_revenue_by_channel[daily_revenue_by_channel['media_source'].isin(top_channels)]
                
                # Pivot for plotting
                pivot_df = daily_revenue_by_channel.pivot(index='date', columns='media_source', values='total_revenue').fillna(0)
                pivot_df = pivot_df.sort_index()
                
                # Plot each channel
                for channel in pivot_df.columns:
                    ax3.plot(pivot_df.index, pivot_df[channel], label=channel, marker='o', linewidth=2, markersize=4)
                
                ax3.set_title('Daily Revenue by Acquisition Channel', fontsize=16, fontweight='bold')
                ax3.set_ylabel('Revenue ($)', fontsize=12)
                ax3.set_xlabel('Date', fontsize=12)
                ax3.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
                ax3.grid(True, alpha=0.3)
                ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                ax3.xaxis.set_major_locator(mdates.DayLocator(interval=2))
                plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
            else:
                ax3.text(0.5, 0.5, 'Acquisition channel revenue data not available', 
                        ha='center', va='center', transform=ax3.transAxes, fontsize=12)
                ax3.set_title('Daily Revenue by Acquisition Channel', fontsize=16, fontweight='bold')
            
            # Daily Revenue by Geography (using country from aggregate table)
            if df_agg is not None and 'country' in df_agg.columns and 'total_revenue' in df_agg.columns:
                # Calculate daily total revenue by country
                daily_revenue_by_country = df_agg.groupby(['date', 'country'])['total_revenue'].sum().reset_index()
                
                # Get top countries (limit to top 10 to avoid clutter)
                top_countries = daily_revenue_by_country.groupby('country')['total_revenue'].sum().nlargest(10).index.tolist()
                daily_revenue_by_country = daily_revenue_by_country[daily_revenue_by_country['country'].isin(top_countries)]
                
                # Pivot for plotting
                pivot_df = daily_revenue_by_country.pivot(index='date', columns='country', values='total_revenue').fillna(0)
                pivot_df = pivot_df.sort_index()
                
                # Plot each country
                for country in pivot_df.columns:
                    ax4.plot(pivot_df.index, pivot_df[country], label=country, marker='o', linewidth=2, markersize=4)
                
                ax4.set_title('Daily Revenue by Geography', fontsize=16, fontweight='bold')
                ax4.set_ylabel('Revenue ($)', fontsize=12)
                ax4.set_xlabel('Date', fontsize=12)
                ax4.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
                ax4.grid(True, alpha=0.3)
                ax4.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                ax4.xaxis.set_major_locator(mdates.DayLocator(interval=2))
                plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)
            else:
                ax4.text(0.5, 0.5, 'Geographic revenue data not available', 
                        ha='center', va='center', transform=ax4.transAxes, fontsize=12)
                ax4.set_title('Daily Revenue by Geography', fontsize=16, fontweight='bold')
            
            plt.tight_layout()
            
            # Save chart
            chart_path = f"{self.output_dir}/revenue_trend.png"
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"✅ Revenue trend chart saved: {chart_path}")
            return chart_path
            
        except Exception as e:
            print(f"❌ Error creating revenue trend chart: {e}")
            import traceback
            traceback.print_exc()
            return ""
    
    def create_arpu_performance_chart(self) -> str:
        """Create ARPU performance charts: overall ARPU and ARPU by acquisition channel."""
        try:
            # Load aggregated data for ARPU calculation
            agg_file = f"{self.data_dir}/aggregations/aggregated_data.csv"
            if not os.path.exists(agg_file):
                print(f"⚠️ Aggregated data file not found: {agg_file}")
                return ""
                
            df = pd.read_csv(agg_file)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                # Filter to last complete date if set
                if hasattr(self, 'last_complete_date') and self.last_complete_date is not None:
                    df = df[df['date'] <= self.last_complete_date]
            
            # Check required columns
            if 'total_revenue' not in df.columns or 'user_id' not in df.columns:
                print(f"⚠️ Required columns (total_revenue, user_id) not found in aggregated data")
                return ""
            
            # Create figure with 2 subplots
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
            
            # 1. Day-wise Overall Average Revenue Per User (ARPU)
            if 'date' in df.columns:
                # Calculate daily ARPU: total revenue / unique users per day
                daily_arpu = df.groupby('date').agg({
                    'total_revenue': 'sum',
                    'user_id': 'nunique'
                }).reset_index()
                daily_arpu['arpu'] = daily_arpu['total_revenue'] / daily_arpu['user_id']
                daily_arpu = daily_arpu[daily_arpu['arpu'] > 0]  # Remove days with zero ARPU (optional)
                
                ax1.plot(daily_arpu['date'], daily_arpu['arpu'], marker='o', linewidth=2, markersize=6, color='purple')
                ax1.set_title('Daily Average Revenue Per User (ARPU)', fontsize=16, fontweight='bold')
                ax1.set_ylabel('ARPU ($)', fontsize=12)
                ax1.set_xlabel('Date', fontsize=12)
                ax1.grid(True, alpha=0.3)
                ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))
                plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            else:
                ax1.text(0.5, 0.5, 'Date column not available', 
                        ha='center', va='center', transform=ax1.transAxes, fontsize=12)
                ax1.set_title('Daily Average Revenue Per User (ARPU)', fontsize=16, fontweight='bold')
            
            # 2. Day-wise ARPU by Acquisition Channel (using media_source)
            if 'media_source' in df.columns and 'date' in df.columns:
                # Calculate daily ARPU by media_source
                daily_arpu_by_channel = df.groupby(['date', 'media_source']).agg({
                    'total_revenue': 'sum',
                    'user_id': 'nunique'
                }).reset_index()
                daily_arpu_by_channel['arpu'] = daily_arpu_by_channel['total_revenue'] / daily_arpu_by_channel['user_id']
                daily_arpu_by_channel = daily_arpu_by_channel[daily_arpu_by_channel['arpu'] > 0]
                
                # Get top channels (limit to top 10 to avoid clutter)
                top_channels = daily_arpu_by_channel.groupby('media_source')['total_revenue'].sum().nlargest(10).index.tolist()
                daily_arpu_by_channel = daily_arpu_by_channel[daily_arpu_by_channel['media_source'].isin(top_channels)]
                
                # Pivot for plotting
                pivot_df = daily_arpu_by_channel.pivot(index='date', columns='media_source', values='arpu').fillna(0)
                pivot_df = pivot_df.sort_index()
                
                # Plot each channel
                for channel in pivot_df.columns:
                    ax2.plot(pivot_df.index, pivot_df[channel], label=channel, marker='o', linewidth=2, markersize=4)
                
                ax2.set_title('Daily ARPU by Acquisition Channel', fontsize=16, fontweight='bold')
                ax2.set_ylabel('ARPU ($)', fontsize=12)
                ax2.set_xlabel('Date', fontsize=12)
                ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
                ax2.grid(True, alpha=0.3)
                ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                ax2.xaxis.set_major_locator(mdates.DayLocator(interval=2))
                plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            else:
                ax2.text(0.5, 0.5, 'Acquisition channel data not available', 
                        ha='center', va='center', transform=ax2.transAxes, fontsize=12)
                ax2.set_title('Daily ARPU by Acquisition Channel', fontsize=16, fontweight='bold')
            
            plt.tight_layout()
            
            # Save chart
            chart_path = f"{self.output_dir}/arpu_performance.png"
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"✅ ARPU performance chart saved: {chart_path}")
            return chart_path
            
        except Exception as e:
            print(f"❌ Error creating ARPU performance chart: {e}")
            import traceback
            traceback.print_exc()
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
            
            # Filter to last complete date if set
            if hasattr(self, 'last_complete_date') and self.last_complete_date is not None:
                df = df[df['date'] <= self.last_complete_date]
            
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
            
            # Filter to last complete date if set (event funnel uses date column for filtering)
            if hasattr(self, 'last_complete_date') and self.last_complete_date is not None:
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    df = df[df['date'] <= self.last_complete_date]
            
            # Calculate event funnel metrics
            total_users = len(df)
            
            # App opened (users with any events)
            app_opened = len(df[df['total_events'] > 0])
            
            # FTUE completed (users with ftue_complete_time)
            ftue_completed = len(df[df['ftue_complete_time'].notna()])
            
            # First level completed (users with level_1_time)
            first_level_completed = len(df[df['level_1_time'].notna()])
            
            # Second level completed (users with level_2_time)
            second_level_completed = len(df[df['level_2_time'].notna()])
            
            # Third level completed (users with level_3_time)
            third_level_completed = len(df[df['level_3_time'].notna()])
            
            # Ad watched count: Users with first_purchase_time (any revenue) but first_iap_purchase_time is null
            # This indicates their first purchase/revenue event was an ad watch
            # We need to check at user level, not user-day level
            if 'first_purchase_time' in df.columns and 'first_iap_purchase_time' in df.columns and 'ad_revenue' in df.columns:
                # Get unique users and check their first purchase type
                # Aggregate to user level: take min of first_purchase_time, check if first_iap_purchase_time exists, sum ad_revenue
                user_first_purchase = df.groupby('user_id').agg({
                    'first_purchase_time': lambda x: x[x.notna()].min() if x.notna().any() else None,
                    'first_iap_purchase_time': lambda x: x.notna().any(),
                    'ad_revenue': 'sum'
                }).reset_index()
                
                # Users whose first purchase was ad (have first_purchase_time but no first_iap_purchase_time and have ad revenue)
                ad_watched_users = len(user_first_purchase[
                    (user_first_purchase['first_purchase_time'].notna()) & 
                    (~user_first_purchase['first_iap_purchase_time']) &
                    (user_first_purchase['ad_revenue'] > 0)
                ])
            else:
                ad_watched_users = 0
            
            # First IAP purchase count: Users with first_iap_purchase_time
            if 'first_iap_purchase_time' in df.columns:
                first_iap_purchase_count = df[df['first_iap_purchase_time'].notna()]['user_id'].nunique()
            else:
                first_iap_purchase_count = 0
            
            # Create funnel data (in correct order)
            funnel_data = [
                total_users,
                app_opened,
                ftue_completed,
                first_level_completed,
                second_level_completed,
                third_level_completed,
                ad_watched_users,
                first_iap_purchase_count
            ]
            
            funnel_labels = [
                'Total Users',
                'App Opened',
                'FTUE Completed',
                'Level 1 Completed',
                'Level 2 Completed',
                'Level 3 Completed',
                'Ad Watched',
                'First IAP Purchase'
            ]
            
            # Calculate conversion rates (all relative to App Opened, i.e., funnel_data[1])
            conversion_rates = []
            app_opened_count = funnel_data[1]  # App Opened is the denominator for all conversion rates
            for i in range(1, len(funnel_data)):
                if app_opened_count > 0:
                    rate = (funnel_data[i] / app_opened_count) * 100
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
        
        if charts.get('arpu_performance'):
            summary.append("### 📊 ARPU Performance")
            summary.append("![ARPU Performance](visualizations/arpu_performance.png)")
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

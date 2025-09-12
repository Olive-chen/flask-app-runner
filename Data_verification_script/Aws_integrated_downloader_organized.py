#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AWS统合データダウンローダー（フォルダ管理版）
DynamoDB と Timestream からデータをダウンロードし、整理されたフォルダに保存
■ 使い方
python aws_integrated_downloader_organized.py --user "azlm-prd-004@01ive.co.jp" --company "azlm-prd" --start "2025-09-04 23:00:00" --end "2025-09-04 23:30:00"
"""

import boto3
import pandas as pd
import json
import argparse
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr
import os
import sys
import logging
from typing import List, Dict, Optional, Tuple
from botocore.exceptions import NoCredentialsError, ClientError, BotoCoreError

class OrganizedAWSDownloader:
    def __init__(self, csv_key_path='shibo-chen_accessKeys.csv', region_name='ap-northeast-1'):
        """
        整理されたAWSダウンローダーの初期化
        """
        self.csv_key_path = csv_key_path
        self.region_name = region_name
        self.dynamodb_table_name = 'dev_lacause_emotion_user_tracking_v1'
        self.timestream_database = 'prd_lacause_emotion_data_v1'
        self.timestream_table = 'lacause_emotion'
        
        # ログ設定
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # AWS認証とクライアント初期化
        self._setup_aws_clients()
        
    def _setup_aws_clients(self):
        """CSVファイルからAWS認証情報を読み込みクライアントを設定"""
        try:
            if not os.path.exists(self.csv_key_path):
                raise FileNotFoundError(f"AWSキーファイルが見つかりません: {self.csv_key_path}")
            
            keys_df = pd.read_csv(self.csv_key_path)
            if keys_df.empty:
                raise ValueError("CSVファイルが空です")
            
            access_key_id = keys_df.iloc[0]['Access key ID']
            secret_access_key = keys_df.iloc[0]['Secret access key']
            
            self.logger.info(f"AWSキーを読み込みました: {access_key_id[:10]}...")
            
            # 各種AWSクライアントを初期化
            self._init_dynamodb_client(access_key_id, secret_access_key)
            self._init_timestream_client(access_key_id, secret_access_key)
            
        except Exception as e:
            self.logger.error(f"AWS認証設定エラー: {e}")
            sys.exit(1)
    
    def _init_dynamodb_client(self, access_key_id, secret_access_key):
        """DynamoDBクライアントの初期化"""
        try:
            self.dynamodb_resource = boto3.resource(
                'dynamodb',
                region_name=self.region_name,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key
            )
            
            self.dynamodb_client = boto3.client(
                'dynamodb',
                region_name=self.region_name,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key
            )
            
            self.dynamodb_table = self.dynamodb_resource.Table(self.dynamodb_table_name)
            
            # 接続テスト
            table_info = self.dynamodb_client.describe_table(TableName=self.dynamodb_table_name)
            self.dynamodb_available = True
            self.logger.info(f"✅ DynamoDBに接続成功: {self.dynamodb_table_name}")
            
        except Exception as e:
            self.dynamodb_available = False
            self.logger.warning(f"⚠️ DynamoDB接続失敗: {e}")
    
    def _init_timestream_client(self, access_key_id, secret_access_key):
        """Timestreamクライアントの初期化"""
        try:
            self.timestream_client = boto3.client(
                'timestream-query',
                region_name=self.region_name,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key
            )
            
            # 接続テスト（簡単なクエリ）
            test_query = f'SELECT COUNT(*) FROM "{self.timestream_database}"."{self.timestream_table}" WHERE time > ago(1d) LIMIT 1'
            self.timestream_client.query(QueryString=test_query)
            
            self.timestream_available = True
            self.logger.info(f"✅ Timestreamに接続成功: {self.timestream_database}.{self.timestream_table}")
            
        except Exception as e:
            self.timestream_available = False
            self.logger.warning(f"⚠️ Timestream接続失敗: {e}")
    
    def create_output_folder(self, start_time, end_time, company=None):
        """出力フォルダを作成"""
        # 実行時間
        request_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # データ期間
        try:
            start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
            end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
            
            if start_dt.date() == end_dt.date():
                # 同じ日の場合
                date_range = start_dt.strftime("%Y%m%d")
            else:
                # 異なる日の場合
                date_range = f"{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}"
        except:
            date_range = "unknown_date"
        
        # フォルダ名作成
        if company:
            folder_name = f"{company}_{date_range}_requested_{request_time}"
        else:
            folder_name = f"data_{date_range}_requested_{request_time}"
        
        # フォルダ作成
        try:
            os.makedirs(folder_name, exist_ok=True)
            self.logger.info(f"📁 出力フォルダ作成: {folder_name}")
            return folder_name
        except Exception as e:
            self.logger.error(f"フォルダ作成エラー: {e}")
            return "."
    
    def _convert_time_to_timestamp(self, time_str):
        """時間文字列をUnixタイムスタンプに変換（ミリ秒単位）"""
        try:
            dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            timestamp_ms = int(dt.timestamp() * 1000)
            return timestamp_ms
        except Exception as e:
            self.logger.error(f"時間変換エラー: {e}")
            return None
    
    def _normalize_user_id(self, user_id):
        """ユーザーIDの正規化"""
        if user_id and "01live.co.jp" in user_id:
            corrected = user_id.replace("01live.co.jp", "01ive.co.jp")
            self.logger.info(f"ユーザーID修正: {user_id} → {corrected}")
            return corrected
        return user_id
    
    # ==================== DynamoDB関連メソッド ====================
    
    def download_from_dynamodb(self, user_id=None, start_time=None, end_time=None, 
                              company=None, event_type=None):
        """DynamoDBからデータをダウンロード"""
        if not self.dynamodb_available:
            self.logger.error("DynamoDBが利用できません")
            return []
        
        self.logger.info("📊 DynamoDBからデータをダウンロード中...")
        
        # ユーザーIDの正規化
        user_id = self._normalize_user_id(user_id)
        
        if not user_id:
            self.logger.error("DynamoDB用のユーザーIDが指定されていません")
            return []
        
        try:
            # ユーザー存在チェック
            response = self.dynamodb_table.query(
                KeyConditionExpression=Key('user_id').eq(user_id),
                Limit=1
            )
            
            if response['Count'] == 0:
                self.logger.warning(f"DynamoDBでユーザー '{user_id}' が見つかりません")
                return []
            
            # 基本キー条件
            key_condition = Key('user_id').eq(user_id)
            
            # 時間範囲の追加（inserted_timeベース）
            if start_time and end_time:
                start_timestamp = self._convert_time_to_timestamp(start_time)
                end_timestamp = self._convert_time_to_timestamp(end_time)
                
                if start_timestamp and end_timestamp:
                    # マージンを追加
                    margin_ms = 60000  # 1分
                    adjusted_start = start_timestamp - margin_ms
                    adjusted_end = end_timestamp + margin_ms
                    
                    key_condition = key_condition & Key('inserted_time').between(adjusted_start, adjusted_end)
            
            # クエリ実行
            items = []
            query_kwargs = {'KeyConditionExpression': key_condition}
            
            while True:
                response = self.dynamodb_table.query(**query_kwargs)
                items.extend(response['Items'])
                
                if 'LastEvaluatedKey' not in response:
                    break
                query_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            
            # 追加フィルタリング
            if start_time and end_time and items:
                filtered_items = []
                for item in items:
                    item_time = item.get('time', '')
                    if item_time and start_time <= item_time <= end_time:
                        filtered_items.append(item)
                items = filtered_items
            
            if company or event_type:
                filtered_items = []
                for item in items:
                    if company and item.get('company') != company:
                        continue
                    if event_type and item.get('event') != event_type:
                        continue
                    filtered_items.append(item)
                items = filtered_items
            
            self.logger.info(f"✅ DynamoDBから {len(items)} 件取得")
            return items
            
        except Exception as e:
            self.logger.error(f"❌ DynamoDBダウンロードエラー: {e}")
            return []
    
    # ==================== Timestream関連メソッド ====================
    
    def _query_timestream_with_pagination(self, query):
        """Timestreamでページネーション対応クエリを実行"""
        try:
            next_token = None
            all_rows = []
            column_info = None
            
            while True:
                # クエリの実行
                if next_token:
                    response = self.timestream_client.query(QueryString=query, NextToken=next_token)
                else:
                    response = self.timestream_client.query(QueryString=query)
                
                # 初回のみColumnInfoを取得
                if column_info is None:
                    column_info = response['ColumnInfo']
                
                # 取得したデータを追加
                all_rows.extend(response['Rows'])
                
                # 次のページがあるか確認
                next_token = response.get('NextToken')
                if not next_token:
                    break
            
            return all_rows, column_info
            
        except ClientError as e:
            self.logger.error(f"Timestreamクライアントエラー: {e.response['Error']['Message']}")
            return None, None
        except Exception as e:
            self.logger.error(f"Timestream予期しないエラー: {str(e)}")
            return None, None
    
    def download_from_timestream(self, company=None, start_time=None, end_time=None, timezone_offset=9):
        """Timestreamからデータをダウンロード"""
        if not self.timestream_available:
            self.logger.error("Timestreamが利用できません")
            return pd.DataFrame()
        
        if not company:
            self.logger.error("Timestream用の会社IDが指定されていません")
            return pd.DataFrame()
        
        self.logger.info("⏱️ Timestreamからデータをダウンロード中...")
        
        try:
            # クエリ構築
            query = f'''
            SELECT 
                time + {timezone_offset}h AS time, 
                user_id, 
                stress, 
                attention, 
                four_types 
            FROM "{self.timestream_database}"."{self.timestream_table}" 
            WHERE (bin(time,10s) + {timezone_offset}h) BETWEEN '{start_time}' AND '{end_time}'
            AND "company" = '{company}'
            ORDER BY time
            '''
            
            self.logger.info(f"Timestreamクエリ実行中...")
            
            # クエリ実行
            response, column_info = self._query_timestream_with_pagination(query)
            
            if not response:
                self.logger.warning("Timestreamクエリの結果が0件です")
                return pd.DataFrame()
            
            # データを整形
            processed_data = []
            for entry in response:
                processed_row = [item['ScalarValue'] for item in entry['Data']]
                processed_data.append(processed_row)
            
            # DataFrameの作成
            columns = ['time', 'user_id', 'stress', 'attention', 'four_types']
            df = pd.DataFrame(processed_data, columns=columns)
            
            # 型の変換
            df['time'] = pd.to_datetime(df['time']).dt.floor('S')
            df['stress'] = pd.to_numeric(df['stress'], errors='coerce')
            df['attention'] = pd.to_numeric(df['attention'], errors='coerce')
            df['four_types'] = pd.to_numeric(df['four_types'], errors='coerce')
            
            # 日時情報を追加
            df['day'] = df['time'].dt.strftime('%Y/%m/%d')
            df['hour'] = df['time'].dt.hour
            df['minutes'] = df['time'].dt.minute
            
            # 列の並び替え
            df = df[['time', 'day', 'hour', 'minutes', 'stress', 'attention', 'four_types', 'user_id']]
            
            self.logger.info(f"✅ Timestreamから {len(df)} 件取得")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Timestreamダウンロードエラー: {e}")
            return pd.DataFrame()
    
    # ==================== データ保存関連メソッド ====================
    
    def save_dynamodb_data(self, items, output_folder, filename_base):
        """DynamoDBデータをCSVのみ保存"""
        if not items:
            self.logger.info("DynamoDBデータが空のため保存をスキップ")
            return []
        
        saved_files = []
        
        try:
            # CSV保存のみ
            df = pd.json_normalize(items)
            csv_filename = os.path.join(output_folder, f"{filename_base}_dynamodb.csv")
            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            saved_files.append(csv_filename)
            
            file_size = os.path.getsize(csv_filename) / 1024
            self.logger.info(f"💾 DynamoDB CSV保存: {csv_filename} ({file_size:.1f} KB)")
            
        except Exception as e:
            self.logger.error(f"DynamoDBデータ保存エラー: {e}")
        
        return saved_files
    
    def save_timestream_data(self, df, output_folder, filename_base):
        """TimestreamデータをCSVのみ保存"""
        if df.empty:
            self.logger.info("Timestreamデータが空のため保存をスキップ")
            return []
        
        saved_files = []
        
        try:
            # CSV保存のみ
            csv_filename = os.path.join(output_folder, f"{filename_base}_timestream.csv")
            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            saved_files.append(csv_filename)
            
            file_size = os.path.getsize(csv_filename) / 1024
            self.logger.info(f"💾 Timestream CSV保存: {csv_filename} ({file_size:.1f} KB)")
            
            
        except Exception as e:
            self.logger.error(f"Timestreamデータ保存エラー: {e}")
        
        return saved_files
    
    def download_all_data(self, user_id=None, company=None, start_time=None, end_time=None, 
                         event_type=None, timezone_offset=9):
        """すべてのデータソースから統合ダウンロード"""
        
        self.logger.info("🚀 統合データダウンロード開始")
        self.logger.info(f"📋 条件: ユーザー={user_id}, 会社={company}, 時間={start_time}~{end_time}")
        
        # 出力フォルダ作成
        output_folder = self.create_output_folder(start_time, end_time, company)
        
        # ファイル名ベース作成
        if company:
            filename_base = f"{company}"
        else:
            filename_base = "data"
        
        all_saved_files = []
        
        # 1. DynamoDBからダウンロード
        if self.dynamodb_available:
            try:
                self.logger.info("📊 DynamoDBデータ処理開始...")
                dynamodb_items = self.download_from_dynamodb(
                    user_id=user_id, 
                    start_time=start_time, 
                    end_time=end_time,
                    company=company, 
                    event_type=event_type
                )
                saved_files = self.save_dynamodb_data(dynamodb_items, output_folder, filename_base)
                all_saved_files.extend(saved_files)
            except Exception as e:
                self.logger.error(f"DynamoDB処理でエラー: {e}")
        
        # 2. Timestreamからダウンロード
        if self.timestream_available and company:
            try:
                self.logger.info("⏱️ Timestreamデータ処理開始...")
                timestream_df = self.download_from_timestream(
                    company=company,
                    start_time=start_time,
                    end_time=end_time,
                    timezone_offset=timezone_offset
                )
                saved_files = self.save_timestream_data(timestream_df, output_folder, filename_base)
                all_saved_files.extend(saved_files)
            except Exception as e:
                self.logger.error(f"Timestream処理でエラー: {e}")
        
        # READMEファイル作成
        self._create_readme_file(output_folder, user_id, company, start_time, end_time, all_saved_files)
        
        # 結果サマリー
        self.logger.info("="*60)
        self.logger.info("🎉 統合ダウンロード完了")
        self.logger.info(f"📁 出力フォルダ: {output_folder}")
        if all_saved_files:
            self.logger.info("📄 保存されたファイル:")
            for file in all_saved_files:
                file_size = os.path.getsize(file) / 1024 if os.path.exists(file) else 0
                rel_path = os.path.relpath(file, output_folder)
                self.logger.info(f"  - {rel_path} ({file_size:.1f} KB)")
        else:
            self.logger.warning("⚠️ 保存されたファイルがありません")
        self.logger.info("="*60)
        
        return output_folder, all_saved_files
    
    def _create_readme_file(self, output_folder, user_id, company, start_time, end_time, saved_files):
        """READMEファイルを作成"""
        try:
            readme_path = os.path.join(output_folder, "README.txt")
            with open(readme_path, 'w', encoding='utf-8') as f:
                f.write("AWS データダウンロード結果\n")
                f.write("=" * 40 + "\n\n")
                
                f.write("📋 ダウンロード条件:\n")
                f.write(f"  ユーザーID: {user_id or 'N/A'}\n")
                f.write(f"  会社ID: {company or 'N/A'}\n")
                f.write(f"  開始時間: {start_time}\n")
                f.write(f"  終了時間: {end_time}\n")
                f.write(f"  実行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("📄 生成されたファイル:\n")
                for file in saved_files:
                    rel_path = os.path.relpath(file, output_folder)
                    file_size = os.path.getsize(file) / 1024 if os.path.exists(file) else 0
                    f.write(f"  - {rel_path} ({file_size:.1f} KB)\n")
                
                f.write("\n📊 ファイル説明:\n")
                f.write("  - *_dynamodb.csv: DynamoDBからの顔検出データ\n")
                f.write("  - *_timestream.csv: Timestreamからの感情データ\n")
            
            self.logger.info(f"📋 README作成: {readme_path}")
            
        except Exception as e:
            self.logger.error(f"README作成エラー: {e}")

def parse_arguments():
    """コマンドライン引数の解析"""
    parser = argparse.ArgumentParser(
        description='AWS統合データダウンローダー（整理版）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 両方のデータソースからダウンロード
  python %(prog)s --user "azlm-prd-004@01ive.co.jp" --company "azlm-prd" --start "2025-09-04 23:00:00" --end "2025-09-04 23:30:00"
  
  # DynamoDBのみ
  python %(prog)s --user "azlm-prd-004@01ive.co.jp" --start "2025-09-04 23:00:00" --end "2025-09-04 23:30:00"
  
  # Timestreamのみ
  python %(prog)s --company "azlm-prd" --start "2025-09-04 23:00:00" --end "2025-09-04 23:30:00"
        """)
    
    parser.add_argument('--user', '-u', type=str, help='ユーザーID（DynamoDB用）')
    parser.add_argument('--company', '-c', type=str, help='会社ID（両方で使用）')
    parser.add_argument('--start', '-s', type=str, required=True, help='開始時間（YYYY-MM-DD HH:MM:SS形式）')
    parser.add_argument('--end', '-e', type=str, required=True, help='終了時間（YYYY-MM-DD HH:MM:SS形式）')
    parser.add_argument('--event', type=str, help='イベントタイプ（DynamoDB用、例: face_detected）')
    parser.add_argument('--timezone', type=int, default=9, help='タイムゾーンオフセット（Timestream用、デフォルト: 9（日本））')
    parser.add_argument('--csv-key-path', type=str, default='shibo-chen_accessKeys.csv',
                        help='AWSキーCSVファイルのパス')
    parser.add_argument('--region', type=str, default='ap-northeast-1',
                        help='AWSリージョン')
    
    return parser.parse_args()

def main():
    """メイン実行関数"""
    args = parse_arguments()
    
    # 統合ダウンローダー初期化
    downloader = OrganizedAWSDownloader(
        csv_key_path=args.csv_key_path,
        region_name=args.region
    )
    
    # 利用可能なサービス確認
    services = []
    if downloader.dynamodb_available:
        services.append("DynamoDB")
    if downloader.timestream_available:
        services.append("Timestream")
    
    if not services:
        print("❌ 利用可能なAWSサービスがありません")
        sys.exit(1)
    
    print(f"\n🔗 利用可能サービス: {', '.join(services)}")
    
    # 統合ダウンロード実行
    output_folder, saved_files = downloader.download_all_data(
        user_id=args.user,
        company=args.company,
        start_time=args.start,
        end_time=args.end,
        event_type=args.event,
        timezone_offset=args.timezone
    )
    
    if saved_files:
        print(f"\n✅ ダウンロード成功！")
        print(f"📁 フォルダ: {output_folder}")
        print(f"📄 ファイル数: {len(saved_files)}")
        print(f"\n💡 フォルダ内容を確認: ls {output_folder}/")
    else:
        print(f"\n❌ ダウンロードできるデータがありませんでした")
        print(f"📁 フォルダは作成されました: {output_folder}")

if __name__ == "__main__":
    main()

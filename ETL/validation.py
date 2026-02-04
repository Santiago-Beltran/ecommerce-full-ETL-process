import re
import sqlite3
from datetime import datetime
from typing import List, Tuple, Dict, Any, Set

class ValidationResult:
    def __init__(self):
        self.valid_records = []
        self.rejected_records = []
        self.errors = []
        self.warnings = []

class DataValidator:
    def __init__(self, conn_olap: sqlite3.Connection, run_id: int):
        self.conn_olap = conn_olap
        self.run_id = run_id
        self.valid_payment_types = {'visa', 'mastercard', 'wire transfer', 'other'}
        self.valid_statuses = {'success', 'failed'}
        self.email_pattern = re.compile(r'^[^\s@]+@[^\s@]+\.[^\s@]+$')

    def log_error(self, entity: str, table_name: str, record_id: str, 
                  error_type: str, message: str, severity: str = 'error'):
        self.conn_olap.execute("""
            INSERT INTO etl_error_log (
                run_id, entity, table_name, record_id, error_type, 
                message, created_at, severity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (self.run_id, entity, table_name, str(record_id), error_type, 
               message, datetime.now().isoformat(), severity))

    def validate_users(self, users: List[Tuple]) -> ValidationResult:
        result = ValidationResult()
        
        for user in users:
            user_id, name, email, join_date = user
            is_valid = True
            
            if not name or not name.strip():
                self.log_error('user', 'users', str(user_id), 'invalid_user', 
                              f'Empty name for user {user_id}')
                is_valid = False
                
            if not email or not self.email_pattern.match(email):
                self.log_error('user', 'users', str(user_id), 'invalid_user',
                              f'Invalid email "{email}" for user {user_id}')
                is_valid = False
                
            if join_date is None:
                self.log_error('user', 'users', str(user_id), 'invalid_user',
                              f'NULL join_date for user {user_id}')
                is_valid = False
                
            if is_valid:
                result.valid_records.append(user)
            else:
                result.rejected_records.append(user)
                
        return result

    def validate_products(self, products: List[Tuple]) -> ValidationResult:
        result = ValidationResult()
        
        for product in products:
            product_id, name, category, price, stock = product
            is_valid = True
            
            if not name or not name.strip():
                self.log_error('product', 'products', str(product_id), 'invalid_product',
                              f'Empty name for product {product_id}')
                is_valid = False
                
            if price >= 10000:
                self.log_error('product', 'products', str(product_id), 'price_ge_10000',
                              f'Product {product_id} price {price} >= 10000')
                is_valid = False
                
            if stock < 0:
                self.log_error('product', 'products', str(product_id), 'negative_stock',
                              f'Product {product_id} has negative stock {stock}')
                is_valid = False
                
            if is_valid:
                result.valid_records.append(product)
            else:
                result.rejected_records.append(product)
                
        return result

    def _parse_date(self, date_str: str) -> str:
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date().strftime('%Y-%m-%d')
        except ValueError:
            try:
                if '/' in date_str:
                    return datetime.strptime(date_str, '%Y/%m/%d').date().strftime('%Y-%m-%d')
                elif 'T' in date_str:
                    return datetime.fromisoformat(date_str.split('T')[0]).date().strftime('%Y-%m-%d')
                elif date_str.isdigit() and len(date_str) == 8:
                    return datetime.strptime(date_str, '%Y%m%d').date().strftime('%Y-%m-%d')
                else:
                    raise ValueError("Unparseable date")
            except ValueError:
                raise ValueError(f"Cannot parse date: {date_str}")

    def validate_transactions(self, transactions: List[Tuple], 
                            valid_user_ids: Set[int], 
                            valid_product_ids: Set[int], 
                            product_prices: Dict[int, float]) -> ValidationResult:
        result = ValidationResult()
        seen_tx_ids = set()
        
        for transaction in transactions:
            tx_id, tx_date_str, user_id, product_id, quantity, total_price, payment_type, status = transaction
            is_valid = True
            
            if user_id not in valid_user_ids:
                self.log_error('transaction', 'transactions', str(tx_id), 'orphan_user',
                              f'Transaction {tx_id} references non-existent user {user_id}')
                is_valid = False
                
            if product_id not in valid_product_ids:
                self.log_error('transaction', 'transactions', str(tx_id), 'orphan_product',
                              f'Transaction {tx_id} references non-existent product {product_id}')
                is_valid = False
                
            if quantity == 0:
                self.log_error('transaction', 'transactions', str(tx_id), 'qty_zero',
                              f'Transaction {tx_id} has zero quantity')
                is_valid = False
            elif quantity < 0:
                self.log_error('transaction', 'transactions', str(tx_id), 'qty_negative',
                              f'Transaction {tx_id} has negative quantity {quantity}')
                is_valid = False
                
            payment_type_normalized = payment_type.lower() if payment_type else ''
            if payment_type_normalized not in self.valid_payment_types:
                self.log_error('transaction', 'transactions', str(tx_id), 'invalid_payment_type',
                              f'Transaction {tx_id} has invalid payment_type "{payment_type}"')
                is_valid = False
                
            status_normalized = status.lower() if status else ''
            if status_normalized not in self.valid_statuses:
                self.log_error('transaction', 'transactions', str(tx_id), 'invalid_status',
                              f'Transaction {tx_id} has invalid status "{status}"')
                is_valid = False
                
            try:
                parsed_date_str = self._parse_date(tx_date_str)
            except ValueError:
                self.log_error('transaction', 'transactions', str(tx_id), 'bad_date_format',
                              f'Transaction {tx_id} has unparseable date "{tx_date_str}"')
                is_valid = False
                parsed_date_str = tx_date_str
                
            if tx_id in seen_tx_ids:
                self.log_error('transaction', 'transactions', str(tx_id), 'duplicate_tx_id',
                              f'Duplicate transaction_id {tx_id}', 'warning')
            else:
                seen_tx_ids.add(tx_id)
                
            if product_id in product_prices and is_valid and quantity > 0:
                expected_price = product_prices[product_id]
                if abs(total_price / quantity - expected_price) > 0.01:
                    self.log_error('transaction', 'transactions', str(tx_id), 'price_mismatch',
                                  f'Transaction {tx_id} price mismatch: expected {expected_price}, got {total_price/quantity}', 'warning')
                    
            if is_valid:
                normalized_transaction = (
                    tx_id, parsed_date_str, user_id, product_id, 
                    quantity, total_price, payment_type_normalized, status_normalized
                )
                result.valid_records.append(normalized_transaction)
            else:
                result.rejected_records.append(transaction)
                
        return result

    def validate_all(self, users: List[Tuple], products: List[Tuple], 
                    transactions: List[Tuple]) -> Dict[str, ValidationResult]:
        user_result = self.validate_users(users)
        product_result = self.validate_products(products)
        
        valid_user_ids = {user[0] for user in user_result.valid_records}
        valid_product_ids = {product[0] for product in product_result.valid_records}
        product_prices = {product[0]: product[3] for product in product_result.valid_records}
        
        transaction_result = self.validate_transactions(
            transactions, valid_user_ids, valid_product_ids, product_prices
        )
        
        return {
            'users': user_result,
            'products': product_result,
            'transactions': transaction_result
        }
# Dataset description

The CSV files contain a normalized extract from an anonymized dataset used for the Sapiens Solutions course project. Store, receipt, material, promotion and coupon identifiers refer only to the training dataset; the data contains no customer names or contact details.

The source Excel file was converted into the repository schema as follows:

| CSV | Excel sheet | Transformation |
| --- | --- | --- |
| `stores.csv` | `Магазины` (Stores) | Store code and a neutral display name |
| `promo_types.csv` | `Тип акции` (Promo type) | Promotion type code and description |
| `promos.csv` | `Акции` (Promotions) | Promotion, material, type and discount amount |
| `bills_head.csv` | `Чеки` (Receipts) | One row per receipt with store and date |
| `bills_item.csv` | `Чеки` (Receipts) | Rows grouped by receipt and material; revenue and quantity summed |
| `traffic.csv` | `Трафик` (Traffic) | Hourly intervals aggregated into daily store traffic |
| `coupons.csv` | `coupons` | Coupon link kept only where a receipt line item and a promotion exist |

The source workbook is not included in the repository. This keeps the example compact and avoids distributing the stale connection credentials that were present in the course documents.

# ETLorderdata
#Cleaning/Transformation Tasks You’ll Perform

| Task                        | Column(s) Affected                  | Details                                              |
| --------------------------- | ----------------------------------- | ---------------------------------------------------- |
| Date Format Standardization | `order_date`                        | Convert all to `YYYY-MM-DD`                          |
| Whitespace & Case Cleanup   | `customer_name`, `state`, `country` | Trim and standardize case                            |
| Email Validation            | `email`                             | Flag invalid or empty emails                         |
| Currency Normalization      | `price_usd`                         | Remove quotes, convert to float                      |
| Category Correction         | `product_category`                  | Standardize values like "sports", "sportswear", etc. |
| Duplicate Removal           | Entire row                          | Based on `order_id`                                  |
| Grouping/Derivation         | Derived Columns                     | Add CLV, new/returning flag                          |

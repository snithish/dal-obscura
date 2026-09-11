import type { Asset, PolicyRule } from "./api";

export const demoAsset: Asset = {
  id: "demo-customer-revenue",
  catalog: "analytics",
  name: "retail.customer_revenue",
  backend: "iceberg",
  table_identifier: "retail.customer_revenue",
  owners: ["group:finance-governance"],
  schema_fields: [
    { name: "customer_id", type: "long", nullable: false },
    { name: "customer_name", type: "string", nullable: true },
    { name: "email", type: "string", nullable: true },
    { name: "region", type: "string", nullable: true },
    { name: "account_owner", type: "string", nullable: true },
    { name: "annual_revenue", type: "double", nullable: true },
    { name: "profile", type: "struct<contact:struct<email:string,phone:string>>", nullable: true },
  ],
};

export const demoRules: PolicyRule[] = [
  {
    ordinal: 10,
    effect: "allow",
    principals: ["group:us-analysts"],
    columns: ["customer_id", "region", "email"],
    masks: { email: { type: "email" } },
    row_filter: "region = 'US'",
  },
];

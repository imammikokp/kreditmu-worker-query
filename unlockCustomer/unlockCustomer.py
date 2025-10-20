
# produce cache
class Customer:
    id: int
    mobile_phone: str
    legal_name: str
    full_name: str

print("""SELECT customers.id,
       DEC_B64('SEC',customers.mobile_phone) AS mobile_phone,
       DEC_B64('SEC',customers.legal_name)   AS legal_name,
       DEC_B64('SEC',customers.full_name)    AS full_name
FROM "customers"
WHERE is_lock_account = true
  AND "customers"."deleted_at" IS NULL
ORDER BY "customers"."id"
LIMIT 100""")
# // result
customer = Customer()
customer.id = 1
customer.mobile_phone = "081234567890"
customer.legal_name = "Budi"
customer.full_name = "Budi Santoso"

#worker consume
# step 6
print(f"""UPDATE "customers" SET "attempt_cs_verify"=0,"is_lock_account"=false,"updated_at"='2025-10-17 14:32:02.624' WHERE id = {customer.id}""")



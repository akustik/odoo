import clevercsv
import csv

def parse_codimpuesto (row):
   source = row["codimpuesto"]
   if source == "IVA0" :
      return 'No sujeto Repercutido (Bienes)'
   if source == "IVA10" :
      return 'IVA 10% (Bienes)'
   # VERIFY 
   if source == "IVA10,5" :
      return 'IVA 10,5% (Bienes)'      
   if source == "IVA21" :
      return 'IVA 21% (Bienes)'
   if source == "IVA4" :
      return 'IVA 4% (Bienes)' 
   raise Exception("IVA mapping not found for " + source)

# VERIFY: Missing "observaciones"
# Blat inflat amb mel 250 gr, ECOBÀSICS
# Bossa roba egarenca, COTÖ ROIG
# Oli 500 ml Oblit, CELLER CAN MORRAL
# Cigró menut sac 500 gr, AGRÀRIA DEL VALLÉS
# Moscatell d&#39;Ullastrell 75cl, CELLER ULLASTRELL
# La Modernista, Sanmy
# Xampinyó laminat
# Mongeta vermella
# Vermut d&#39;ullastrell 750 ml, EL CELLER D&#39;ULLASTRELL
def parse_observaciones (row, field, dict, default_value):
   source = row["observaciones"]
   try:
        result = [a.split("=")[1] for a in source.split(";") if field in a]
        if len(result) == 1:
            key = result[0] or default_value
            return dict.get(key, key)
   except Exception as err:
        print("Cannot parse " + str(source) + " for " + field + ". " + row["Nombre"])
   return "N/A"

def parse_stock(row):
    source = row["stockfis"]
    if(source) >= 0:
        return source
    else:
        return 0

df = clevercsv.read_dataframe("articulos.csv", delimiter=",", quotechar="\"", escapechar="", header=0).rename(columns={
    "pvp": "Precio de venta",
    "codbarras": "Código de barras",
    "descripcion": "Nombre",
    "costemedio": "Coste",
    "referencia": "Referencia interna",    
})

# VERIFY
df["Activo"] = df.apply (lambda row: row["bloqueado"] == 0, axis=1)
# VERIFY
df["Puede publicar"] = df.apply (lambda row: row["bloqueado"] == 0, axis=1)

df["Puede ser comprado"] = df.apply (lambda row: row["secompra"] == 1, axis=1)
df["Puede ser vendido"] = df.apply (lambda row: row["sevende"] == 1, axis=1)

# Verify: What to set here?
df["Descripción"] = df.apply (lambda row: row["observaciones"], axis=1)
df["Impuestos del cliente"] = df.apply (lambda row: parse_codimpuesto(row), axis=1)

# VERIFY: Visible? Multiplicador? Origen?
df["Unidad de medida"] = df.apply (lambda row: parse_observaciones(row, "Unitat_Venda", {"unitat": "Unidades"}, "Unidades"), axis=1)
df["Unidad de medida compra"] = df.apply (lambda row: parse_observaciones(row, "Unitat_Compra", {"unitat": "Unidades"}, "Unidades"), axis=1)
df["Descripción de compra"] = df.apply (lambda row: parse_observaciones(row, "Nom_Compra", {}, ""), axis=1)

# VERIFY: Condition for not setting provider taxes
df["Impuestos de proveedor"] = df.apply (lambda row: "21% IVA soportado (bienes corrientes)", axis=1)
# VERIFY: Condition for "Consumible" or other kinds
df["Tipo de producto"] = df.apply (lambda row: "Almacenable", axis=1)
# VERIFY: What options and how to decide?
df["Categoría del Producto"] = df.apply (lambda row: "Sin categoría", axis=1)

df["Cantidad a mano"] = df.apply(lambda row: parse_stock(row), axis=1)

res_df = df[[
    "Activo",
    "Puede publicar",
    "Descripción",
    "Unidad de medida",
    "Unidad de medida compra",
    "Descripción de compra",
    "Código de barras",
    "Impuestos del cliente",
    "Impuestos de proveedor",
    "Cantidad a mano",
    "Coste",
    "Nombre",
    "Puede ser comprado",
    "Categoría del Producto",
    "Referencia interna",
    "Tipo de producto",
    "Precio de venta",
    "Puede ser vendido",
]]


(res_df[(res_df["Unidad de medida"] != "N/A") & (res_df["Unidad de medida compra"] != "N/A") & (res_df["Descripción de compra"] != "N/A")]
.to_csv("articulos.odoo.csv", index=False, quoting=csv.QUOTE_ALL))


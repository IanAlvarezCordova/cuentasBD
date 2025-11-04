# ¿Qué hace este programa?

1.- Lee todos los id de clientes desde la tabla cliente
2.- Divide los clientes en grupos: 1, 2, 3 o 4 cuentas"
3.- Genera cuentas con saldo aleatorio ≤ 7000
4.- Inserta todas las cuentas en la BD (en batch, eficiente)"
5.- Muestra resumen con GROUP BY en SQL

#Ventajas

No repite cuentas (controla id manualmente).
Transacciones seguras (rollback si falla).
Batch insert → rápido incluso con 100k cuentas.
Funciona con cualquier número de clientes.
Fácil de ejecutar como script.

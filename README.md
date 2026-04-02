# Valorizador de Prestaciones Médicas SMG

Esta aplicación automatiza el cruce de datos entre reportes de liquidación y bases de valorización médica.

## Funcionalidades
1. **Regla Nomenclador**: Busca por código y multiplica coeficientes de cirujano por el valor de unidad del mes.
2. **Regla Valores Fijos**: Busca importes directos filtrando por nomencladores específicos y categorías.
3. **Búsqueda Global**: Última instancia de búsqueda en base de fijos sin filtros de nomenclador.
4. **Limpieza Automática**: Elimina duplicados por `transacción_item` y genera columna de Total.

## Cómo usar
1. Sube el archivo de liquidación.
2. Sube la base de datos de valorización (Excel con pestañas: Nomenclador, unidades, Valor Fijos).
3. Presiona procesar y descarga el resultado.

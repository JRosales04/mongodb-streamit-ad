# Streamit: Diseño y migración de datos en MongoDB

# :ok_man: Autores

-   Javier Rosales Lozano
-   Alonso Ríos Guerra
-   Nahuel Sebastián Vargas
-   Alejandro Rodríguez García

Última modificación: 12/01/2026

## :globe_with_meridians: Descripción

Trabajo de prácticas (2025-2026) para la asignatura Arquitectura de Datos del Grado de Ingeniería Informática.

Universidad Carlos III de Madrid.

## :book: Resumen

Práctica de diseño de arquitectura, implementación, migración y tratamiento de datos para una empresa ficticia de suscripción de televisión bajo demanda (películas y series).

El proyecto global consta de dos partes:

<table>
  <thead>
    <tr>
      <th>Fase</th>
      <th>Descripción</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>1. Diseño en MongoDB</td>
      <td>
        En esta fase se realiza el modelado de datos para la empresa StreamIt S.A., partiendo de los casos de uso definidos (historial de consumos, fidelización en series, análisis por país, rankings de actores y películas y resumen mensual de facturación). En esta fase se diseña:
        <ol>
          <li>El modelo UML conceptual y lógico, reflejando entidades, relaciones, atributos y cardinalidades necesarias para dar soporte a los casos de uso</li>
          <li>El diseño de agregados en MongoDB, justificando la elección de raíces de agregados, qué elementos se embeben y cuáles se referencian, y aplicando reglas de consistencia (formato de fechas, normalización de nombres y apellidos, manejo de valores nulos, duplicados, etc.)</li>
        </ol>
      </td>
    </tr>
    <tr>
      <td>2. Migración y tratamiento de datos</td>
      <td>
        En esta fase se trabaja sobre un dataset de facturas con datos inconsistentes y sucios, realizando un proceso de limpieza, normalización y transformación de los datos directamente en MongoDB, aplicando reglas de consistencia para fechas, nombres, apellidos, campos ausentes y duplicados. Además, se reestructura el modelo de datos, separando la información en colecciones de películas y series, mientras que la colección de facturas se simplifica y mantiene referencias a los contenidos, adaptándose al modelo UML anterior y optimizando la explotación posterior mediante agregaciones.
      </td>
    </tr>
  </tbody>
</table>

## :construction: Estructura del proyecto

```plaintext
mongodb-streamit-ad/
├── code/                           # Scripts y código en MongoDB
│   ├── 1_limpieza.js               # Limpieza y normalización de datos
│   ├── 2_reestructuracion.js       # Separación de facturas en documentos
│   ├── 3_validacion_esquemas.js    # Validación de esquemas
│   ├── 4_agregaciones.js           # Consultas de agregación
│   └── datafiles.zip               # Dataset raw/inicial
├── docs/                           # Memorias de trabajo y enunciados
├── imgs/                           # Diagrama UML y de agregados
└── README.md                       # Este archivo
```

1. `part2/code/1_limpieza.js` — detecta/normaliza nombres de campos, limpia valores nulos/duplicados. Tras esto, la colección `facturas` estará normalizada en nombres de campos y tipos.
2. `part2/code/2_reestructuracion.js` — extrae referencias a películas/series y crea colecciones derivadas (`Peliculas`, `Series`). Se generarán las colecciones `Peliculas` y `Series`, y `facturas` contendrá referencias simplificadas.
3. `part2/code/3_validacion_esquemas.js` — aplica validaciones de esquema y revisiones sobre `facturas` y colecciones derivadas. No se mostrará cambio en los datos, ya que afecta solo a los nuevos introducidos.
4. `part2/code/4_agregaciones.js` — consultas analíticas y agregaciones requeridas por el enunciado.

## :link: Dependencias

<img src="https://go-skill-icons.vercel.app/api/icons?i=mongodb,js&theme=dark&titles=true"/>

-   Instalación de MongoDB (servidor) compatible; los scripts usan operaciones de agregación modernas. Se recomienda **MongoDB 4.4+ o 5.x**. Para este proyecto, se ha utilizado la interfaz oficial [MongoDB Compass](https://www.mongodb.com/products/tools/compass).
-   `mongosh` o `mongo` para ejecutar los scripts desde la línea de comandos.

## :inbox_tray: Instalación y uso

1. **Clonar el repositorio**.

```bash
git clone https://github.com/JRosales04/mongodb-streamit-ad.git
```

2. **Crear la base de datos original**.

```shell
# 1. Descomprimir el dataset
unzip code/datafiles.zip -d code/datafiles
# 2. Importar los datos a MongoDB
mongoimport --db facturas --collection facturas --file code/datafiles/facturas.json --jsonArray
# 3. Entrar al shell de MongoDB
mongosh
# 4. Seleccionar la base de datos
use facturas
# 5. Comprobar que los datos se importaron
db.facturas.find().limit(5)
```

3. **Ejecutar los scripts de MongoDB en orden**.

```shell
# Script 1: Limpieza y normalización de datos
mongosh code/1_limpieza.js
# Script 2: Reestructuración de la colección de facturas en colecciones de Películas y Series
mongosh code/2_reestructuracion.js
# Script 3: Validación de esquemas y reglas de consistencia
mongosh code/3_validacion_esquemas.js
# Script 4: Consultas de agregación de ejemplo para los casos de uso
mongosh code/4_agregaciones.js
```

Para comprobar el resultado de los scripts:

```shell
# Entrar al shell de MongoDB
mongosh
# Seleccionar la base de datos
use facturas
# Ver las colecciones creadas
show collections
# Revisar algunos documentos de ejemplo
db.facturas.find().limit(5)
db.Peliculas.find().limit(5)
db.Series.find().limit(5)
```

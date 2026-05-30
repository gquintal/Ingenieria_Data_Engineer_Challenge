I. Propón una arquitectura (solo proponla no la construyas) que considere los siguientes aspectos y preguntas:
    A. De cada fuente de datos se tienen identificados que campos requiere el área operativa. ¿Para cumplir con los dos objetivos que subconjunto de cada fuente de datos extraerías?
    La extracción se limitaría al subconjunto requerido por negocio, enriquecido únicamente con campos técnicos y analíticos indispensables para consolidación, trazabilidad, auditoria.

    B. ¿Qué posibles retos implica la extracción de cada una de las fuentes de datos por separado y qué herramientas utilizas ?
   Posible duplicidad, inconsistencias, en general la calidad de los datos en cada fuente origen puede ser diferente, es necesario un buen proceso de validación/calidad para los datos relevantes de cada proceso de negocio, asi como establecer reglas y automatizarlas para que se apliquen durante la extracción.

    C. ¿Qué posibles retos implica la independencia en el modelo de datos de las tres fuentes y cómo los resolverías?
    Las llaves que se usan en las diferentes tablas de cada base de datos. Es necesario hacer una tabla de homologación para identificar a quien pertenece cada registro de cada BD.
    Tipos de datos, nomenclaturas, significado de cada datos. Es necesario transformar cada dato en caso de ser necesario para unificar los tipos, significados, etc.

    D. ¿Aparte de un proceso batch en la hora de menor uso, cómo podrías mitigar el impacto de tu pipeline sobre las fuentes originales ? 
    Replicar a una base de datos de solo lectura. Aplicando estrategias como CDC. 
    
    E. ¿Cuáles etapas considerarías en tu proceso de transformación de datos y qué uso les darías?
    Raw: Datos crudos sin modificar, como llegan de la fuente.
    Bronze: Datos parseados y con schema aplicado.
    Silver: Limpieza, deduplicación, modelo canónico.
    Gold: Agregaciones listas para consumo operativo (SQL).
    DataSet: Subconjuntos específicos para ciencia de datos.

    F. ¿Qué herramientas utilizas para las etapas de transformación? 
    Databricks, Python (Pyspark/Polars/Pandas).
    
    G. ¿Qué storage usarías para cada propósito y por qué ?
    Datalake storage y para consultas SQL un Datawarehouse.

    H. Recuerda que al menos a diario tendrás que llevar data nueva a tu etapa de transformación final, ¿Como orquestarias tu pipeline y con qué herramienta?
    Con Airflow o DataFactory programado en los horarios acordados.

    I. Proporciona un diagrama de tu propuesta de arquitectura.
    Se adjunta.


II. Seguridad (manteniendo tu rol de ingeniero de datos).
A. ¿Cómo mantendrías la seguridad de tu flujo de datos end-to-end? Es decir disminuir riesgos de posibles fugas o intrusiones no deseadas al entorno de ejecución que estás construyendo.
Principio de mínimo privilegio, no exponer claves ni secretos, en el diseño identificar los datos que no deberian de estar en claro, utilizar herramientas que permitan dar permisos a nivel columna o fila por ejemplo. Cifrado en transito y en reposo. Asegurar una correcta trazabilidad.

III. Gobernanza de datos
A. ¿Cómo llevarías control de la metadata y sus cambios al igual que los procesos de tu pipeline y cómo almacenamos estos datos?
Proceso de DataQuality, algun sistema de Gobernanza como Unity Catalog en Databricks, en el acceso a SQL solo usuarios autorizados.
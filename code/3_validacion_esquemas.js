// FACTURAS

db.runCommand({
    collMod: "facturas",
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: [
                "_id",
                "billing",
                "TOTAL",
                "Client",
                "charge_date",
                "dump_date",
                "Contract",
                "Movies",
                "Series",
            ],
            properties: {
                _id: {
                    bsonType: "string",
                    description: "ID de la factura, string obligatorio",
                },
                billing: {
                    bsonType: "date",
                    description: "Fecha de facturación de la factura",
                },
                TOTAL: {
                    bsonType: "decimal",
                    description:
                        "Importe total de la factura en formato decimal",
                },
                Client: {
                    bsonType: "object",
                    required: [
                        "birth_date",
                        "customer_code",
                        "dni",
                        "email",
                        "name",
                        "phone",
                        "surname",
                    ],
                    description: "Datos del cliente",
                    properties: {
                        birth_date: {
                            bsonType: "date",
                            description: "Fecha de nacimiento del cliente",
                        },
                        customer_code: {
                            bsonType: "string",
                            pattern: "^[A-Z0-9/]+$",
                            description: "ID del cliente",
                        },
                        dni: {
                            bsonType: "string",
                            pattern: "^[A-Z0-9]+$",
                            description: "DNI del cliente",
                        },
                        email: {
                            bsonType: "string",
                            pattern: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w{2,}$",
                            description: "Correo electrónico válido",
                        },
                        name: {
                            bsonType: "string",
                            pattern: "^[A-ZÑ ]+$",
                            description: "Nombre del cliente",
                        },
                        phone: {
                            bsonType: "number",
                            description: "Número de teléfono del cliente",
                        },
                        surname: {
                            bsonType: "array",
                            items: {
                                bsonType: "string",
                                pattern: "^[A-ZÑ ]+$",
                            },
                            description: "Apellido(s) del cliente",
                            minItems: 1,
                        },
                    },
                },
                charge_date: {
                    bsonType: "date",
                    description: "Fecha de cargo",
                },
                dump_date: {
                    bsonType: "date",
                    description: "Fecha de registro o descarga",
                },
                Contract: {
                    bsonType: "object",
                    required: [
                        "address",
                        "town",
                        "country",
                        "end_date",
                        "contract_id",
                        "start_date",
                        "zip",
                        "Product",
                    ],
                    description: "Detalle del contrato asociado",
                    properties: {
                        address: {
                            bsonType: "string",
                            pattern: "^[A-ZÑ0-9 ,\\-]+$",
                            description: "Dirección en mayúsculas",
                        },
                        town: {
                            bsonType: "string",
                            pattern: "^[A-ZÑ0-9 ]+$",
                            description: "Ciudad en mayúsculas",
                        },
                        country: {
                            bsonType: "string",
                            pattern: "^[A-ZÑ0-9 ]+$",
                            description: "País en mayúsculas",
                        },
                        end_date: {
                            bsonType: ["date", "null"],
                            description: "Fecha de fin de contrato",
                        },
                        contract_id: {
                            bsonType: "string",
                            pattern: "^[A-Z0-9/]+$",
                            description: "ID de contrato",
                        },
                        start_date: {
                            bsonType: "date",
                            description: "Fecha de inicio de contrato",
                        },
                        zip: {
                            bsonType: "number",
                            description: "Código postal",
                        },
                        Product: {
                            bsonType: "object",
                            required: [
                                "type",
                                "zapping",
                                "promotion",
                                "cost_per_content",
                                "cost_per_day",
                                "cost_per_minute",
                                "monthly_fee",
                                "reference",
                            ],
                            description: "Datos del producto contratado",
                            properties: {
                                type: {
                                    bsonType: "string",
                                    pattern: "^[A-Z0-9 ]+$",
                                    description: "Tipo de producto",
                                },
                                zapping: {
                                    bsonType: "number",
                                    description: "Número de zappings",
                                },
                                promotion: {
                                    bsonType: "number",
                                    description: "Valor de promoción",
                                },
                                cost_per_content: {
                                    bsonType: "decimal",
                                    description: "Costo por contenido",
                                },
                                cost_per_day: {
                                    bsonType: "decimal",
                                    description: "Costo diario",
                                },
                                cost_per_minute: {
                                    bsonType: "decimal",
                                    description: "Costo por minuto",
                                },
                                monthly_fee: {
                                    bsonType: "decimal",
                                    description: "Cuota mensual",
                                },
                                reference: {
                                    bsonType: "string",
                                    pattern: "^[A-Z0-9 ]+$",
                                    description: "Referencia en mayúsculas",
                                },
                                cost_per_view: {
                                    bsonType: "decimal",
                                    description: "Costo por vista",
                                },
                            },
                        },
                    },
                },
                Movies: {
                    bsonType: "array",
                    items: {
                        bsonType: "object",
                        required: ["mov_id", "viewing_pct", "date_time"],
                        properties: {
                            mov_id: {
                                bsonType: "string",
                                pattern: "^[A-Z0-9]+$",
                                description: "ID de película",
                            },
                            viewing_pct: {
                                bsonType: "number",
                                description: "Porcentaje de visualización",
                            },
                            date_time: {
                                bsonType: "date",
                                description: "Fecha y hora de visualización",
                            },
                        },
                    },
                },
                Series: {
                    bsonType: "array",
                    items: {
                        bsonType: "object",
                        required: [
                            "serie_id",
                            "license",
                            "season",
                            "episode",
                            "viewing_pct",
                            "date_time",
                        ],
                        properties: {
                            serie_id: {
                                bsonType: "string",
                                pattern: "^[A-Z0-9_\\s]+/[0-9]+$",
                                description: "ID de serie",
                            },
                            license: {
                                bsonType: "object",
                                required: ["date_time"],
                                properties: {
                                    date_time: {
                                        bsonType: "date",
                                        description: "Fecha de licencia",
                                    },
                                },
                            },
                            season: {
                                bsonType: "number",
                                description: "Número de temporada",
                            },
                            episode: {
                                bsonType: "number",
                                description: "Número de episodio",
                            },
                            viewing_pct: {
                                bsonType: "number",
                                description: "Porcentaje de visualización",
                            },
                            date_time: {
                                bsonType: "date",
                                description: "Fecha y hora de visualización",
                            },
                        },
                    },
                },
            },
        },
    },
    validationLevel: "moderate",
    validationAction: "error",
});

// PELICULAS

db.runCommand({
    collMod: "Peliculas",
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["_id", "title", "Details", "License"],
            properties: {
                _id: {
                    bsonType: "string",
                    description: "ID de pelicula",
                    pattern: "^[A-Z0-9:/ ]+$",
                },
                title: {
                    bsonType: "string",
                    description: "Titulo de la pelicula",
                    pattern: "^[A-Z0-9:/ ]+$",
                },
                Details: {
                    bsonType: "object",
                    required: [
                        "aspect_ratio",
                        "budget",
                        "color",
                        "content_rating",
                        "contrato",
                        "critic_reviews",
                        "duration",
                        "facebook_likes",
                        "faces_in_poster",
                        "genres",
                        "gross",
                        "imdb_link",
                        "imdb_score",
                        "keywords",
                        "language",
                        "user_reviews",
                        "voted_users",
                        "year",
                        "Director",
                        "Cast",
                    ],
                    properties: {
                        aspect_ratio: {
                            bsonType: ["double", "int"],
                            description: "Relacion de aspecto",
                        },
                        budget: {
                            bsonType: ["decimal"],
                            description: "Presupuesto de la pelicula",
                        },
                        color: {
                            bsonType: "string",
                            description: "Pelicula en blanco y negro o a color",
                            pattern: "^[A-Z0-9:/ ]+$",
                        },
                        content_rating: {
                            bsonType: "string",
                            description: "Rating de contenido",
                            pattern: "^[A-Z0-9:/ ]+$",
                        },
                        country: {
                            bsonType: "string",
                            description: "Pais de produccion",
                            pattern: "^[A-Z0-9:/ ]+$",
                        },
                        critic_reviews: {
                            bsonType: "int",
                            description: "Reseñas de criticos",
                        },
                        duration: {
                            bsonType: "int",
                            description: "Duracion de la pelicula",
                        },
                        facebook_likes: {
                            bsonType: "int",
                            description: "Likes de Facebook de la pelicula",
                        },
                        faces_in_poster: {
                            bsonType: "int",
                            description: "Caras en poster de la pelicula",
                        },
                        genres: {
                            bsonType: "array",
                            description: "Generos de la pelicula",
                            items: {
                                bsonType: "string",
                                description: "Genero de la pelicula",
                                pattern: "^[A-Z0-9:/ ]+$",
                            },
                            minItems: 1,
                        },
                        gross: {
                            bsonType: "int",
                            description: "Recaudacion total de la pelicula",
                        },
                        imdb_link: {
                            bsonType: "string",
                            description: "Link de pelicula en IMDB",
                            pattern: "^[A-Z0-9:/ ]+$",
                        },
                        imdb_score: {
                            bsonType: "double",
                            description: "Puntuacion de la pelicula",
                        },
                        keywords: {
                            bsonType: "array",
                            description: "Palabras clave",
                            pattern: "^[A-Z0-9:/ ]+$",
                            items: {
                                bsonType: "string",
                                description: "Palabra clave",
                                pattern: "^[A-Z0-9:/ ]+$",
                            },
                            minItems: 1,
                        },
                        language: {
                            bsonType: "string",
                            description: "Idioma de la pelicula",
                            pattern: "^[A-Z0-9:/ ]+$",
                        },
                        user_reviews: {
                            bsonType: "int",
                            description: "Reseñas de usuario",
                        },
                        voted_users: {
                            bsonType: "int",
                            description: "Votos de usuario",
                        },
                        year: {
                            bsonType: "int",
                            description: "Año de estreno",
                        },
                        Director: {
                            bsonType: "object",
                            required: ["facebook_likes", "name"],
                            properties: {
                                facebook_likes: {
                                    bsonType: "int",
                                    description:
                                        "Likes de Facebook del director",
                                },
                                name: {
                                    bsonType: "string",
                                    description: "Nombre del director",
                                    pattern: "^[A-Z0-9:/ ]+$",
                                },
                            },
                        },
                        Cast: {
                            bsonType: "object",
                            required: ["Stars"],
                            properties: {
                                Stars: {
                                    bsonType: "array",
                                    items: {
                                        bsonType: "object",
                                        required: ["facebook_likes", "player"],
                                        properties: {
                                            facebook_likes: {
                                                bsonType: "int",
                                                description:
                                                    "Likes de Facebook del actor/actriz",
                                            },
                                            player: {
                                                bsonType: "string",
                                                description:
                                                    "Nombre del actor/actriz",
                                                pattern: "^[A-Z0-9:/ ]+$",
                                            },
                                        },
                                    },
                                    minItems: 1,
                                },
                            },
                        },
                    },
                },
                License: {
                    bsonType: "object",
                    properties: {
                        date_time: {
                            bsonType: "date",
                            description: "Fecha de licencia",
                        },
                    },
                },
            },
        },
    },
    validationLevel: "moderate",
    validationAction: "error",
});

// SERIES

db.runCommand({
    collMod: "Series",
    validator: {
        $and: [
            {
                $jsonSchema: {
                    bsonType: "object",
                    required: [
                        "_id",
                        "avg_duration",
                        "season_episodes",
                        "title",
                        "total_seasons",
                    ],
                    properties: {
                        _id: {
                            bsonType: "string",
                            description: "ID de la serie",
                            pattern: "^[A-Z0-9_\\s]+/[0-9]+$",
                        },
                        avg_duration: {
                            bsonType: "number",
                            description: "Duración media del episodio",
                        },
                        season_episodes: {
                            bsonType: "array",
                            description: "Array de episodios por temporada",
                            items: { bsonType: "number" },
                            minItems: 1,
                        },
                        title: {
                            bsonType: "string",
                            description: "Título de la serie",
                        },
                        total_seasons: {
                            bsonType: "number",
                            description: "Número de temporadas de la serie",
                        },
                    },
                },
            },
            {
                $expr: {
                    $eq: [{ $size: "$season_episodes" }, "$total_seasons"],
                },
            },
        ],
    },
    validationLevel: "moderate",
    validationAction: "error",
});

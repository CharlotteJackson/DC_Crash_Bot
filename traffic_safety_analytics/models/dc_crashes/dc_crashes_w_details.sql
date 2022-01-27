{{ config(
    materialized = 'table'
    ,indexes=[
      {'columns': ['geography'], 'type': 'GIST'},
      {'columns': ['crimeid'], 'unique': True},
    ]
    ) }}
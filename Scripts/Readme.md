{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyOKYUSoC0cIWsqZ6tnJaI60",
      "include_colab_link": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/josevalladares99/etl-data-pipeline-25-2432-2022/blob/main/Scripts/Readme.md\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": 4,
      "metadata": {
        "id": "C4yz1ckRuXqD"
      },
      "outputs": [],
      "source": [
        "import pandas as pd"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "url=\"https://raw.githubusercontent.com/josevalladares99/etl-data-pipeline-25-2432-2022/refs/heads/main/Data/raw/C_ventas.csv\""
      ],
      "metadata": {
        "id": "Jqe0svgJuo1J"
      },
      "execution_count": 5,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "df=pd.read_csv(url)\n",
        "df.head()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 206
        },
        "id": "cV87eJYmus35",
        "outputId": "c72b7350-821e-409b-e3e6-95bc26e0ea75"
      },
      "execution_count": 6,
      "outputs": [
        {
          "output_type": "execute_result",
          "data": {
            "text/plain": [
              "  id_venta id_cliente       fecha    total\n",
              "0    V9000      CL154  2024-10-25  4641.86\n",
              "1    V9001      CL243  2024-06-29  1138.59\n",
              "2    V9002      CL111  2024-01-25  1873.39\n",
              "3    V9003      CL244  2024-01-26      NaN\n",
              "4    V9004      CL243  2024-05-24  2208.24"
            ],
            "text/html": [
              "\n",
              "  <div id=\"df-209ffe23-17f1-4c1d-8308-7413f0ac71f0\" class=\"colab-df-container\">\n",
              "    <div>\n",
              "<style scoped>\n",
              "    .dataframe tbody tr th:only-of-type {\n",
              "        vertical-align: middle;\n",
              "    }\n",
              "\n",
              "    .dataframe tbody tr th {\n",
              "        vertical-align: top;\n",
              "    }\n",
              "\n",
              "    .dataframe thead th {\n",
              "        text-align: right;\n",
              "    }\n",
              "</style>\n",
              "<table border=\"1\" class=\"dataframe\">\n",
              "  <thead>\n",
              "    <tr style=\"text-align: right;\">\n",
              "      <th></th>\n",
              "      <th>id_venta</th>\n",
              "      <th>id_cliente</th>\n",
              "      <th>fecha</th>\n",
              "      <th>total</th>\n",
              "    </tr>\n",
              "  </thead>\n",
              "  <tbody>\n",
              "    <tr>\n",
              "      <th>0</th>\n",
              "      <td>V9000</td>\n",
              "      <td>CL154</td>\n",
              "      <td>2024-10-25</td>\n",
              "      <td>4641.86</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>1</th>\n",
              "      <td>V9001</td>\n",
              "      <td>CL243</td>\n",
              "      <td>2024-06-29</td>\n",
              "      <td>1138.59</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>2</th>\n",
              "      <td>V9002</td>\n",
              "      <td>CL111</td>\n",
              "      <td>2024-01-25</td>\n",
              "      <td>1873.39</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>3</th>\n",
              "      <td>V9003</td>\n",
              "      <td>CL244</td>\n",
              "      <td>2024-01-26</td>\n",
              "      <td>NaN</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>4</th>\n",
              "      <td>V9004</td>\n",
              "      <td>CL243</td>\n",
              "      <td>2024-05-24</td>\n",
              "      <td>2208.24</td>\n",
              "    </tr>\n",
              "  </tbody>\n",
              "</table>\n",
              "</div>\n",
              "    <div class=\"colab-df-buttons\">\n",
              "\n",
              "  <div class=\"colab-df-container\">\n",
              "    <button class=\"colab-df-convert\" onclick=\"convertToInteractive('df-209ffe23-17f1-4c1d-8308-7413f0ac71f0')\"\n",
              "            title=\"Convert this dataframe to an interactive table.\"\n",
              "            style=\"display:none;\">\n",
              "\n",
              "  <svg xmlns=\"http://www.w3.org/2000/svg\" height=\"24px\" viewBox=\"0 -960 960 960\">\n",
              "    <path d=\"M120-120v-720h720v720H120Zm60-500h600v-160H180v160Zm220 220h160v-160H400v160Zm0 220h160v-160H400v160ZM180-400h160v-160H180v160Zm440 0h160v-160H620v160ZM180-180h160v-160H180v160Zm440 0h160v-160H620v160Z\"/>\n",
              "  </svg>\n",
              "    </button>\n",
              "\n",
              "  <style>\n",
              "    .colab-df-container {\n",
              "      display:flex;\n",
              "      gap: 12px;\n",
              "    }\n",
              "\n",
              "    .colab-df-convert {\n",
              "      background-color: #E8F0FE;\n",
              "      border: none;\n",
              "      border-radius: 50%;\n",
              "      cursor: pointer;\n",
              "      display: none;\n",
              "      fill: #1967D2;\n",
              "      height: 32px;\n",
              "      padding: 0 0 0 0;\n",
              "      width: 32px;\n",
              "    }\n",
              "\n",
              "    .colab-df-convert:hover {\n",
              "      background-color: #E2EBFA;\n",
              "      box-shadow: 0px 1px 2px rgba(60, 64, 67, 0.3), 0px 1px 3px 1px rgba(60, 64, 67, 0.15);\n",
              "      fill: #174EA6;\n",
              "    }\n",
              "\n",
              "    .colab-df-buttons div {\n",
              "      margin-bottom: 4px;\n",
              "    }\n",
              "\n",
              "    [theme=dark] .colab-df-convert {\n",
              "      background-color: #3B4455;\n",
              "      fill: #D2E3FC;\n",
              "    }\n",
              "\n",
              "    [theme=dark] .colab-df-convert:hover {\n",
              "      background-color: #434B5C;\n",
              "      box-shadow: 0px 1px 3px 1px rgba(0, 0, 0, 0.15);\n",
              "      filter: drop-shadow(0px 1px 2px rgba(0, 0, 0, 0.3));\n",
              "      fill: #FFFFFF;\n",
              "    }\n",
              "  </style>\n",
              "\n",
              "    <script>\n",
              "      const buttonEl =\n",
              "        document.querySelector('#df-209ffe23-17f1-4c1d-8308-7413f0ac71f0 button.colab-df-convert');\n",
              "      buttonEl.style.display =\n",
              "        google.colab.kernel.accessAllowed ? 'block' : 'none';\n",
              "\n",
              "      async function convertToInteractive(key) {\n",
              "        const element = document.querySelector('#df-209ffe23-17f1-4c1d-8308-7413f0ac71f0');\n",
              "        const dataTable =\n",
              "          await google.colab.kernel.invokeFunction('convertToInteractive',\n",
              "                                                    [key], {});\n",
              "        if (!dataTable) return;\n",
              "\n",
              "        const docLinkHtml = 'Like what you see? Visit the ' +\n",
              "          '<a target=\"_blank\" href=https://colab.research.google.com/notebooks/data_table.ipynb>data table notebook</a>'\n",
              "          + ' to learn more about interactive tables.';\n",
              "        element.innerHTML = '';\n",
              "        dataTable['output_type'] = 'display_data';\n",
              "        await google.colab.output.renderOutput(dataTable, element);\n",
              "        const docLink = document.createElement('div');\n",
              "        docLink.innerHTML = docLinkHtml;\n",
              "        element.appendChild(docLink);\n",
              "      }\n",
              "    </script>\n",
              "  </div>\n",
              "\n",
              "\n",
              "    </div>\n",
              "  </div>\n"
            ],
            "application/vnd.google.colaboratory.intrinsic+json": {
              "type": "dataframe",
              "variable_name": "df",
              "summary": "{\n  \"name\": \"df\",\n  \"rows\": 248,\n  \"fields\": [\n    {\n      \"column\": \"id_venta\",\n      \"properties\": {\n        \"dtype\": \"string\",\n        \"num_unique_values\": 240,\n        \"samples\": [\n          \"V9024\",\n          \"V9006\",\n          \"V9093\"\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    },\n    {\n      \"column\": \"id_cliente\",\n      \"properties\": {\n        \"dtype\": \"category\",\n        \"num_unique_values\": 115,\n        \"samples\": [\n          \"CL162\",\n          \"CL239\",\n          \"CL233\"\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    },\n    {\n      \"column\": \"fecha\",\n      \"properties\": {\n        \"dtype\": \"string\",\n        \"num_unique_values\": 181,\n        \"samples\": [\n          \"2024-07-06\",\n          \"2024-02-19\",\n          \"2025-01-29\"\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    },\n    {\n      \"column\": \"total\",\n      \"properties\": {\n        \"dtype\": \"string\",\n        \"num_unique_values\": 234,\n        \"samples\": [\n          \"1541.91\",\n          \"2302.34\",\n          \"2105.59\"\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    }\n  ]\n}"
            }
          },
          "metadata": {},
          "execution_count": 6
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "df.info()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "jA0E0jgE4U5Y",
        "outputId": "4c4af94d-fbf2-4e60-c86d-42df593e1db4"
      },
      "execution_count": 7,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "<class 'pandas.core.frame.DataFrame'>\n",
            "RangeIndex: 248 entries, 0 to 247\n",
            "Data columns (total 4 columns):\n",
            " #   Column      Non-Null Count  Dtype \n",
            "---  ------      --------------  ----- \n",
            " 0   id_venta    248 non-null    object\n",
            " 1   id_cliente  242 non-null    object\n",
            " 2   fecha       239 non-null    object\n",
            " 3   total       241 non-null    object\n",
            "dtypes: object(4)\n",
            "memory usage: 7.9+ KB\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "df.isnull().sum()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 210
        },
        "id": "665J9-O_4jDf",
        "outputId": "60ccac53-ace3-4528-f64e-21886dc6f57c"
      },
      "execution_count": 8,
      "outputs": [
        {
          "output_type": "execute_result",
          "data": {
            "text/plain": [
              "id_venta      0\n",
              "id_cliente    6\n",
              "fecha         9\n",
              "total         7\n",
              "dtype: int64"
            ],
            "text/html": [
              "<div>\n",
              "<style scoped>\n",
              "    .dataframe tbody tr th:only-of-type {\n",
              "        vertical-align: middle;\n",
              "    }\n",
              "\n",
              "    .dataframe tbody tr th {\n",
              "        vertical-align: top;\n",
              "    }\n",
              "\n",
              "    .dataframe thead th {\n",
              "        text-align: right;\n",
              "    }\n",
              "</style>\n",
              "<table border=\"1\" class=\"dataframe\">\n",
              "  <thead>\n",
              "    <tr style=\"text-align: right;\">\n",
              "      <th></th>\n",
              "      <th>0</th>\n",
              "    </tr>\n",
              "  </thead>\n",
              "  <tbody>\n",
              "    <tr>\n",
              "      <th>id_venta</th>\n",
              "      <td>0</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>id_cliente</th>\n",
              "      <td>6</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>fecha</th>\n",
              "      <td>9</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>total</th>\n",
              "      <td>7</td>\n",
              "    </tr>\n",
              "  </tbody>\n",
              "</table>\n",
              "</div><br><label><b>dtype:</b> int64</label>"
            ]
          },
          "metadata": {},
          "execution_count": 8
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "df[df.duplicated()]"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 300
        },
        "id": "-nXNl0RS4m8f",
        "outputId": "7dc12316-9d2e-4ea7-f595-8451b06e893e"
      },
      "execution_count": 9,
      "outputs": [
        {
          "output_type": "execute_result",
          "data": {
            "text/plain": [
              "    id_venta id_cliente       fecha    total\n",
              "240    V9080      CL178  2025-01-13   1969.8\n",
              "241    V9129      CL161  2024-07-13  1823.93\n",
              "242    V9003      CL244  2024-01-26      NaN\n",
              "243    V9205      CL107  2024-05-20   591.55\n",
              "244    V9148      CL248  2024-09-13  3645.14\n",
              "245    V9190      CL116  2024-04-25  1297.52\n",
              "246    V9094      CL192  2025-02-21  2812.09\n",
              "247    V9088      CL130  2024-09-29  2949.25"
            ],
            "text/html": [
              "\n",
              "  <div id=\"df-ff5ddf49-f917-4b84-9524-bbf20119b160\" class=\"colab-df-container\">\n",
              "    <div>\n",
              "<style scoped>\n",
              "    .dataframe tbody tr th:only-of-type {\n",
              "        vertical-align: middle;\n",
              "    }\n",
              "\n",
              "    .dataframe tbody tr th {\n",
              "        vertical-align: top;\n",
              "    }\n",
              "\n",
              "    .dataframe thead th {\n",
              "        text-align: right;\n",
              "    }\n",
              "</style>\n",
              "<table border=\"1\" class=\"dataframe\">\n",
              "  <thead>\n",
              "    <tr style=\"text-align: right;\">\n",
              "      <th></th>\n",
              "      <th>id_venta</th>\n",
              "      <th>id_cliente</th>\n",
              "      <th>fecha</th>\n",
              "      <th>total</th>\n",
              "    </tr>\n",
              "  </thead>\n",
              "  <tbody>\n",
              "    <tr>\n",
              "      <th>240</th>\n",
              "      <td>V9080</td>\n",
              "      <td>CL178</td>\n",
              "      <td>2025-01-13</td>\n",
              "      <td>1969.8</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>241</th>\n",
              "      <td>V9129</td>\n",
              "      <td>CL161</td>\n",
              "      <td>2024-07-13</td>\n",
              "      <td>1823.93</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>242</th>\n",
              "      <td>V9003</td>\n",
              "      <td>CL244</td>\n",
              "      <td>2024-01-26</td>\n",
              "      <td>NaN</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>243</th>\n",
              "      <td>V9205</td>\n",
              "      <td>CL107</td>\n",
              "      <td>2024-05-20</td>\n",
              "      <td>591.55</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>244</th>\n",
              "      <td>V9148</td>\n",
              "      <td>CL248</td>\n",
              "      <td>2024-09-13</td>\n",
              "      <td>3645.14</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>245</th>\n",
              "      <td>V9190</td>\n",
              "      <td>CL116</td>\n",
              "      <td>2024-04-25</td>\n",
              "      <td>1297.52</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>246</th>\n",
              "      <td>V9094</td>\n",
              "      <td>CL192</td>\n",
              "      <td>2025-02-21</td>\n",
              "      <td>2812.09</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>247</th>\n",
              "      <td>V9088</td>\n",
              "      <td>CL130</td>\n",
              "      <td>2024-09-29</td>\n",
              "      <td>2949.25</td>\n",
              "    </tr>\n",
              "  </tbody>\n",
              "</table>\n",
              "</div>\n",
              "    <div class=\"colab-df-buttons\">\n",
              "\n",
              "  <div class=\"colab-df-container\">\n",
              "    <button class=\"colab-df-convert\" onclick=\"convertToInteractive('df-ff5ddf49-f917-4b84-9524-bbf20119b160')\"\n",
              "            title=\"Convert this dataframe to an interactive table.\"\n",
              "            style=\"display:none;\">\n",
              "\n",
              "  <svg xmlns=\"http://www.w3.org/2000/svg\" height=\"24px\" viewBox=\"0 -960 960 960\">\n",
              "    <path d=\"M120-120v-720h720v720H120Zm60-500h600v-160H180v160Zm220 220h160v-160H400v160Zm0 220h160v-160H400v160ZM180-400h160v-160H180v160Zm440 0h160v-160H620v160ZM180-180h160v-160H180v160Zm440 0h160v-160H620v160Z\"/>\n",
              "  </svg>\n",
              "    </button>\n",
              "\n",
              "  <style>\n",
              "    .colab-df-container {\n",
              "      display:flex;\n",
              "      gap: 12px;\n",
              "    }\n",
              "\n",
              "    .colab-df-convert {\n",
              "      background-color: #E8F0FE;\n",
              "      border: none;\n",
              "      border-radius: 50%;\n",
              "      cursor: pointer;\n",
              "      display: none;\n",
              "      fill: #1967D2;\n",
              "      height: 32px;\n",
              "      padding: 0 0 0 0;\n",
              "      width: 32px;\n",
              "    }\n",
              "\n",
              "    .colab-df-convert:hover {\n",
              "      background-color: #E2EBFA;\n",
              "      box-shadow: 0px 1px 2px rgba(60, 64, 67, 0.3), 0px 1px 3px 1px rgba(60, 64, 67, 0.15);\n",
              "      fill: #174EA6;\n",
              "    }\n",
              "\n",
              "    .colab-df-buttons div {\n",
              "      margin-bottom: 4px;\n",
              "    }\n",
              "\n",
              "    [theme=dark] .colab-df-convert {\n",
              "      background-color: #3B4455;\n",
              "      fill: #D2E3FC;\n",
              "    }\n",
              "\n",
              "    [theme=dark] .colab-df-convert:hover {\n",
              "      background-color: #434B5C;\n",
              "      box-shadow: 0px 1px 3px 1px rgba(0, 0, 0, 0.15);\n",
              "      filter: drop-shadow(0px 1px 2px rgba(0, 0, 0, 0.3));\n",
              "      fill: #FFFFFF;\n",
              "    }\n",
              "  </style>\n",
              "\n",
              "    <script>\n",
              "      const buttonEl =\n",
              "        document.querySelector('#df-ff5ddf49-f917-4b84-9524-bbf20119b160 button.colab-df-convert');\n",
              "      buttonEl.style.display =\n",
              "        google.colab.kernel.accessAllowed ? 'block' : 'none';\n",
              "\n",
              "      async function convertToInteractive(key) {\n",
              "        const element = document.querySelector('#df-ff5ddf49-f917-4b84-9524-bbf20119b160');\n",
              "        const dataTable =\n",
              "          await google.colab.kernel.invokeFunction('convertToInteractive',\n",
              "                                                    [key], {});\n",
              "        if (!dataTable) return;\n",
              "\n",
              "        const docLinkHtml = 'Like what you see? Visit the ' +\n",
              "          '<a target=\"_blank\" href=https://colab.research.google.com/notebooks/data_table.ipynb>data table notebook</a>'\n",
              "          + ' to learn more about interactive tables.';\n",
              "        element.innerHTML = '';\n",
              "        dataTable['output_type'] = 'display_data';\n",
              "        await google.colab.output.renderOutput(dataTable, element);\n",
              "        const docLink = document.createElement('div');\n",
              "        docLink.innerHTML = docLinkHtml;\n",
              "        element.appendChild(docLink);\n",
              "      }\n",
              "    </script>\n",
              "  </div>\n",
              "\n",
              "\n",
              "    </div>\n",
              "  </div>\n"
            ],
            "application/vnd.google.colaboratory.intrinsic+json": {
              "type": "dataframe",
              "repr_error": "0"
            }
          },
          "metadata": {},
          "execution_count": 9
        }
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "Limpieza de datos"
      ],
      "metadata": {
        "id": "OY5obyVB45EB"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "import re"
      ],
      "metadata": {
        "id": "5Ilj3Ajw46cf"
      },
      "execution_count": 25,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "\n",
        "df['total'] = (\n",
        "    df['total']\n",
        "    .astype(str)\n",
        "    .str.replace('USD', '', regex=False)\n",
        "    .str.replace('$', '', regex=False)\n",
        "    .str.strip()\n",
        ")\n",
        "\n",
        "df['total'] = pd.to_numeric(df['total'], errors='coerce')\n"
      ],
      "metadata": {
        "id": "Avsu97Sl49WH"
      },
      "execution_count": 26,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "df['fecha'] = df['fecha'].astype(str).str.strip()"
      ],
      "metadata": {
        "id": "i1ZR8eEj5A4f"
      },
      "execution_count": 27,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "\n",
        "df['fecha_dt'] = pd.to_datetime(\n",
        "    df['fecha'],\n",
        "    errors='coerce',\n",
        "    infer_datetime_format=True,\n",
        "    dayfirst=True\n",
        ")\n"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "L71jGlii5D3v",
        "outputId": "f2065f83-b5c7-4a1f-cb29-3eafc3987b9c"
      },
      "execution_count": 28,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stderr",
          "text": [
            "/tmp/ipykernel_16140/1708259142.py:1: UserWarning: The argument 'infer_datetime_format' is deprecated and will be removed in a future version. A strict version of it is now the default, see https://pandas.pydata.org/pdeps/0004-consistent-to-datetime-parsing.html. You can safely remove this argument.\n",
            "  df['fecha_dt'] = pd.to_datetime(\n",
            "/tmp/ipykernel_16140/1708259142.py:1: UserWarning: Parsing dates in %Y-%m-%d format when dayfirst=True was specified. Pass `dayfirst=False` or specify a format to silence this warning.\n",
            "  df['fecha_dt'] = pd.to_datetime(\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "df.loc[df['id_cliente'].isin(['', 'nan', 'None']), 'id_cliente'] = pd.NA"
      ],
      "metadata": {
        "id": "nhJQNpqd5HAO"
      },
      "execution_count": 29,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "df['id_cliente'] = df['id_cliente'].astype(str).str.strip()"
      ],
      "metadata": {
        "id": "0QRF7EEr5L3A"
      },
      "execution_count": 30,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "df = df.drop_duplicates()"
      ],
      "metadata": {
        "id": "o13t8eh35OXo"
      },
      "execution_count": 31,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "\n",
        "def es_venta_valida(row):\n",
        "    # id_venta\n",
        "    if pd.isna(row['id_venta']):\n",
        "        return False\n",
        "\n",
        "    # id_cliente\n",
        "    if pd.isna(row['id_cliente']):\n",
        "        return False\n",
        "    if not re.match(r'^CL\\d+$', str(row['id_cliente'])):\n",
        "        return False\n",
        "    if row['id_cliente'] == 'CL999':\n",
        "        return False\n",
        "\n",
        "    # fecha\n",
        "    if pd.isna(row['fecha_dt']):\n",
        "        return False\n",
        "\n",
        "    # total\n",
        "    if pd.isna(row['total']):\n",
        "        return False\n",
        "    if row['total'] <= 0:\n",
        "        return False\n",
        "\n",
        "    return True\n"
      ],
      "metadata": {
        "id": "l0VvaqJE5Qs_"
      },
      "execution_count": 39,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "Separar datos validos y rechazados"
      ],
      "metadata": {
        "id": "mqEyfttX5Wrf"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "\n",
        "\n",
        "ventas_validas = df[df.apply(es_venta_valida, axis=1)]\n",
        "ventas_rechazadas = df[~df.apply(es_venta_valida, axis=1)]\n"
      ],
      "metadata": {
        "id": "9wVsFyyR5Z1X"
      },
      "execution_count": 40,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "Motivo de rechazo"
      ],
      "metadata": {
        "id": "83_NDMRe5dff"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "\n",
        "def motivo_rechazo_venta(row):\n",
        "    motivos = []\n",
        "\n",
        "    if pd.isna(row['id_venta']):\n",
        "        motivos.append('id_venta_nulo')\n",
        "\n",
        "    if pd.isna(row['id_cliente']):\n",
        "        motivos.append('id_cliente_nulo')\n",
        "    elif not re.match(r'^CL\\d+$', str(row['id_cliente'])):\n",
        "        motivos.append('id_cliente_formato_invalido')\n",
        "    elif row['id_cliente'] == 'CL999':\n",
        "        motivos.append('id_cliente_invalido')\n",
        "\n",
        "    if pd.isna(row['fecha_dt']):\n",
        "        motivos.append('fecha_invalida')\n",
        "\n",
        "    if pd.isna(row['total']):\n",
        "        motivos.append('total_nulo')\n",
        "    elif row['total'] <= 0:\n",
        "        motivos.append('total_no_positivo')\n",
        "\n",
        "    return ','.join(motivos)\n",
        "\n"
      ],
      "metadata": {
        "id": "YT9J9_Pr5chf"
      },
      "execution_count": 41,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "ventas_validas = ventas_validas.drop(columns=['fecha'])\n",
        "ventas_validas = ventas_validas.rename(columns={'fecha_dt': 'fecha'})"
      ],
      "metadata": {
        "id": "vTyCklv07M4Q"
      },
      "execution_count": 42,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "Exportar archivos"
      ],
      "metadata": {
        "id": "VpDRiidD5re3"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "ventas_validas.to_csv('ventas_curated.csv', index=False)\n",
        "ventas_rechazadas.to_csv('ventas_rejects.csv', index=False)"
      ],
      "metadata": {
        "id": "LIpiqlw95jE3"
      },
      "execution_count": 43,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "!pip install sqlalchemy psycopg2-binary\n",
        "\n",
        "from sqlalchemy import create_engine\n",
        "import pandas as pd\n",
        "\n",
        "database_url = \"postgresql://etl_user:oBPWTDnwlDkov4SzFc8zpJwu1kTOiolN@dpg-d6qu7s450q8c73bpf590-a.oregon-postgres.render.com/etl_seguros_k3sp\"\n",
        "\n",
        "engine = create_engine(database_url)\n",
        "\n",
        "test = pd.read_sql(\"SELECT 1\", engine)\n",
        "\n",
        "print(test)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "PE8eKP2452le",
        "outputId": "60e3ba38-4aa0-4693-c228-f9135bcad34c"
      },
      "execution_count": 44,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Requirement already satisfied: sqlalchemy in /usr/local/lib/python3.12/dist-packages (2.0.48)\n",
            "Requirement already satisfied: psycopg2-binary in /usr/local/lib/python3.12/dist-packages (2.9.11)\n",
            "Requirement already satisfied: greenlet>=1 in /usr/local/lib/python3.12/dist-packages (from sqlalchemy) (3.3.2)\n",
            "Requirement already satisfied: typing-extensions>=4.6.0 in /usr/local/lib/python3.12/dist-packages (from sqlalchemy) (4.15.0)\n",
            "   ?column?\n",
            "0         1\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "ventas_validas.to_sql(\n",
        "    \"ventas_curated\",\n",
        "    con=engine,\n",
        "    if_exists=\"replace\",\n",
        "    index=False\n",
        ")\n",
        "\n",
        "ventas_rechazadas.to_sql(\n",
        "    \"ventas_rejects\",\n",
        "    con=engine,\n",
        "    if_exists=\"replace\",\n",
        "    index=False\n",
        ")"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "Byy9e4CU6Bpf",
        "outputId": "445278f0-bbdf-44d1-bf76-e265e81ae1fa"
      },
      "execution_count": 45,
      "outputs": [
        {
          "output_type": "execute_result",
          "data": {
            "text/plain": [
              "34"
            ]
          },
          "metadata": {},
          "execution_count": 45
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "pd.read_sql(\n",
        "\"SELECT * FROM ventas_curated LIMIT 10\",\n",
        "engine)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 363
        },
        "id": "X1gtiipQ6Ek_",
        "outputId": "f90c85fc-7798-4fd1-ca7d-3355b4f6e921"
      },
      "execution_count": 46,
      "outputs": [
        {
          "output_type": "execute_result",
          "data": {
            "text/plain": [
              "  id_venta id_cliente    total      fecha\n",
              "0    V9000      CL154  4641.86 2024-10-25\n",
              "1    V9001      CL243  1138.59 2024-06-29\n",
              "2    V9002      CL111  1873.39 2024-01-25\n",
              "3    V9004      CL243  2208.24 2024-05-24\n",
              "4    V9005      CL239  3072.44 2024-09-10\n",
              "5    V9006      CL235  4716.52 2024-11-01\n",
              "6    V9007      CL102  1218.65 2025-03-07\n",
              "7    V9008      CL243   625.08 2024-11-30\n",
              "8    V9009      CL129  1003.40 2024-12-10\n",
              "9    V9010      CL122  4436.89 2024-08-28"
            ],
            "text/html": [
              "\n",
              "  <div id=\"df-0227eecb-8dfc-472d-a445-b31ca46c2bd5\" class=\"colab-df-container\">\n",
              "    <div>\n",
              "<style scoped>\n",
              "    .dataframe tbody tr th:only-of-type {\n",
              "        vertical-align: middle;\n",
              "    }\n",
              "\n",
              "    .dataframe tbody tr th {\n",
              "        vertical-align: top;\n",
              "    }\n",
              "\n",
              "    .dataframe thead th {\n",
              "        text-align: right;\n",
              "    }\n",
              "</style>\n",
              "<table border=\"1\" class=\"dataframe\">\n",
              "  <thead>\n",
              "    <tr style=\"text-align: right;\">\n",
              "      <th></th>\n",
              "      <th>id_venta</th>\n",
              "      <th>id_cliente</th>\n",
              "      <th>total</th>\n",
              "      <th>fecha</th>\n",
              "    </tr>\n",
              "  </thead>\n",
              "  <tbody>\n",
              "    <tr>\n",
              "      <th>0</th>\n",
              "      <td>V9000</td>\n",
              "      <td>CL154</td>\n",
              "      <td>4641.86</td>\n",
              "      <td>2024-10-25</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>1</th>\n",
              "      <td>V9001</td>\n",
              "      <td>CL243</td>\n",
              "      <td>1138.59</td>\n",
              "      <td>2024-06-29</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>2</th>\n",
              "      <td>V9002</td>\n",
              "      <td>CL111</td>\n",
              "      <td>1873.39</td>\n",
              "      <td>2024-01-25</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>3</th>\n",
              "      <td>V9004</td>\n",
              "      <td>CL243</td>\n",
              "      <td>2208.24</td>\n",
              "      <td>2024-05-24</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>4</th>\n",
              "      <td>V9005</td>\n",
              "      <td>CL239</td>\n",
              "      <td>3072.44</td>\n",
              "      <td>2024-09-10</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>5</th>\n",
              "      <td>V9006</td>\n",
              "      <td>CL235</td>\n",
              "      <td>4716.52</td>\n",
              "      <td>2024-11-01</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>6</th>\n",
              "      <td>V9007</td>\n",
              "      <td>CL102</td>\n",
              "      <td>1218.65</td>\n",
              "      <td>2025-03-07</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>7</th>\n",
              "      <td>V9008</td>\n",
              "      <td>CL243</td>\n",
              "      <td>625.08</td>\n",
              "      <td>2024-11-30</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>8</th>\n",
              "      <td>V9009</td>\n",
              "      <td>CL129</td>\n",
              "      <td>1003.40</td>\n",
              "      <td>2024-12-10</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>9</th>\n",
              "      <td>V9010</td>\n",
              "      <td>CL122</td>\n",
              "      <td>4436.89</td>\n",
              "      <td>2024-08-28</td>\n",
              "    </tr>\n",
              "  </tbody>\n",
              "</table>\n",
              "</div>\n",
              "    <div class=\"colab-df-buttons\">\n",
              "\n",
              "  <div class=\"colab-df-container\">\n",
              "    <button class=\"colab-df-convert\" onclick=\"convertToInteractive('df-0227eecb-8dfc-472d-a445-b31ca46c2bd5')\"\n",
              "            title=\"Convert this dataframe to an interactive table.\"\n",
              "            style=\"display:none;\">\n",
              "\n",
              "  <svg xmlns=\"http://www.w3.org/2000/svg\" height=\"24px\" viewBox=\"0 -960 960 960\">\n",
              "    <path d=\"M120-120v-720h720v720H120Zm60-500h600v-160H180v160Zm220 220h160v-160H400v160Zm0 220h160v-160H400v160ZM180-400h160v-160H180v160Zm440 0h160v-160H620v160ZM180-180h160v-160H180v160Zm440 0h160v-160H620v160Z\"/>\n",
              "  </svg>\n",
              "    </button>\n",
              "\n",
              "  <style>\n",
              "    .colab-df-container {\n",
              "      display:flex;\n",
              "      gap: 12px;\n",
              "    }\n",
              "\n",
              "    .colab-df-convert {\n",
              "      background-color: #E8F0FE;\n",
              "      border: none;\n",
              "      border-radius: 50%;\n",
              "      cursor: pointer;\n",
              "      display: none;\n",
              "      fill: #1967D2;\n",
              "      height: 32px;\n",
              "      padding: 0 0 0 0;\n",
              "      width: 32px;\n",
              "    }\n",
              "\n",
              "    .colab-df-convert:hover {\n",
              "      background-color: #E2EBFA;\n",
              "      box-shadow: 0px 1px 2px rgba(60, 64, 67, 0.3), 0px 1px 3px 1px rgba(60, 64, 67, 0.15);\n",
              "      fill: #174EA6;\n",
              "    }\n",
              "\n",
              "    .colab-df-buttons div {\n",
              "      margin-bottom: 4px;\n",
              "    }\n",
              "\n",
              "    [theme=dark] .colab-df-convert {\n",
              "      background-color: #3B4455;\n",
              "      fill: #D2E3FC;\n",
              "    }\n",
              "\n",
              "    [theme=dark] .colab-df-convert:hover {\n",
              "      background-color: #434B5C;\n",
              "      box-shadow: 0px 1px 3px 1px rgba(0, 0, 0, 0.15);\n",
              "      filter: drop-shadow(0px 1px 2px rgba(0, 0, 0, 0.3));\n",
              "      fill: #FFFFFF;\n",
              "    }\n",
              "  </style>\n",
              "\n",
              "    <script>\n",
              "      const buttonEl =\n",
              "        document.querySelector('#df-0227eecb-8dfc-472d-a445-b31ca46c2bd5 button.colab-df-convert');\n",
              "      buttonEl.style.display =\n",
              "        google.colab.kernel.accessAllowed ? 'block' : 'none';\n",
              "\n",
              "      async function convertToInteractive(key) {\n",
              "        const element = document.querySelector('#df-0227eecb-8dfc-472d-a445-b31ca46c2bd5');\n",
              "        const dataTable =\n",
              "          await google.colab.kernel.invokeFunction('convertToInteractive',\n",
              "                                                    [key], {});\n",
              "        if (!dataTable) return;\n",
              "\n",
              "        const docLinkHtml = 'Like what you see? Visit the ' +\n",
              "          '<a target=\"_blank\" href=https://colab.research.google.com/notebooks/data_table.ipynb>data table notebook</a>'\n",
              "          + ' to learn more about interactive tables.';\n",
              "        element.innerHTML = '';\n",
              "        dataTable['output_type'] = 'display_data';\n",
              "        await google.colab.output.renderOutput(dataTable, element);\n",
              "        const docLink = document.createElement('div');\n",
              "        docLink.innerHTML = docLinkHtml;\n",
              "        element.appendChild(docLink);\n",
              "      }\n",
              "    </script>\n",
              "  </div>\n",
              "\n",
              "\n",
              "    </div>\n",
              "  </div>\n"
            ],
            "application/vnd.google.colaboratory.intrinsic+json": {
              "type": "dataframe",
              "summary": "{\n  \"name\": \"engine)\",\n  \"rows\": 10,\n  \"fields\": [\n    {\n      \"column\": \"id_venta\",\n      \"properties\": {\n        \"dtype\": \"string\",\n        \"num_unique_values\": 10,\n        \"samples\": [\n          \"V9009\",\n          \"V9001\",\n          \"V9006\"\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    },\n    {\n      \"column\": \"id_cliente\",\n      \"properties\": {\n        \"dtype\": \"string\",\n        \"num_unique_values\": 8,\n        \"samples\": [\n          \"CL243\",\n          \"CL102\",\n          \"CL154\"\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    },\n    {\n      \"column\": \"total\",\n      \"properties\": {\n        \"dtype\": \"number\",\n        \"std\": 1609.2624066820179,\n        \"min\": 625.08,\n        \"max\": 4716.52,\n        \"num_unique_values\": 10,\n        \"samples\": [\n          1003.4,\n          1138.59,\n          4716.52\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    },\n    {\n      \"column\": \"fecha\",\n      \"properties\": {\n        \"dtype\": \"date\",\n        \"min\": \"2024-01-25 00:00:00\",\n        \"max\": \"2025-03-07 00:00:00\",\n        \"num_unique_values\": 10,\n        \"samples\": [\n          \"2024-12-10 00:00:00\",\n          \"2024-06-29 00:00:00\",\n          \"2024-11-01 00:00:00\"\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    }\n  ]\n}"
            }
          },
          "metadata": {},
          "execution_count": 46
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "pd.read_sql(\n",
        "\"SELECT * FROM ventas_rejects LIMIT 10\",\n",
        "engine)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 363
        },
        "id": "nSsUJcwM7atf",
        "outputId": "fa13c9df-4121-46fa-fd9e-de6e3f8cf2d3"
      },
      "execution_count": 47,
      "outputs": [
        {
          "output_type": "execute_result",
          "data": {
            "text/plain": [
              "  id_venta id_cliente       fecha    total   fecha_dt\n",
              "0    V9003      CL244  2024-01-26      NaN 2024-01-26\n",
              "1    V9012      CL214         nan  1443.82        NaT\n",
              "2    V9013      CL203  2024/99/10  4083.42        NaT\n",
              "3    V9025      CL115  31/15/2024  2734.23        NaT\n",
              "4    V9028      CL135  2024-10-14      NaN 2024-10-14\n",
              "5    V9035      CL166         nan  4206.70        NaT\n",
              "6    V9040       <NA>  2025-02-26  1501.79 2025-02-26\n",
              "7    V9057      CL190  2024-13-01  3670.70        NaT\n",
              "8    V9059      CL119         nan   956.36        NaT\n",
              "9    V9061      CL124         nan  3923.28        NaT"
            ],
            "text/html": [
              "\n",
              "  <div id=\"df-4189312c-c9c7-4167-81be-d7a5fa6e9530\" class=\"colab-df-container\">\n",
              "    <div>\n",
              "<style scoped>\n",
              "    .dataframe tbody tr th:only-of-type {\n",
              "        vertical-align: middle;\n",
              "    }\n",
              "\n",
              "    .dataframe tbody tr th {\n",
              "        vertical-align: top;\n",
              "    }\n",
              "\n",
              "    .dataframe thead th {\n",
              "        text-align: right;\n",
              "    }\n",
              "</style>\n",
              "<table border=\"1\" class=\"dataframe\">\n",
              "  <thead>\n",
              "    <tr style=\"text-align: right;\">\n",
              "      <th></th>\n",
              "      <th>id_venta</th>\n",
              "      <th>id_cliente</th>\n",
              "      <th>fecha</th>\n",
              "      <th>total</th>\n",
              "      <th>fecha_dt</th>\n",
              "    </tr>\n",
              "  </thead>\n",
              "  <tbody>\n",
              "    <tr>\n",
              "      <th>0</th>\n",
              "      <td>V9003</td>\n",
              "      <td>CL244</td>\n",
              "      <td>2024-01-26</td>\n",
              "      <td>NaN</td>\n",
              "      <td>2024-01-26</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>1</th>\n",
              "      <td>V9012</td>\n",
              "      <td>CL214</td>\n",
              "      <td>nan</td>\n",
              "      <td>1443.82</td>\n",
              "      <td>NaT</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>2</th>\n",
              "      <td>V9013</td>\n",
              "      <td>CL203</td>\n",
              "      <td>2024/99/10</td>\n",
              "      <td>4083.42</td>\n",
              "      <td>NaT</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>3</th>\n",
              "      <td>V9025</td>\n",
              "      <td>CL115</td>\n",
              "      <td>31/15/2024</td>\n",
              "      <td>2734.23</td>\n",
              "      <td>NaT</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>4</th>\n",
              "      <td>V9028</td>\n",
              "      <td>CL135</td>\n",
              "      <td>2024-10-14</td>\n",
              "      <td>NaN</td>\n",
              "      <td>2024-10-14</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>5</th>\n",
              "      <td>V9035</td>\n",
              "      <td>CL166</td>\n",
              "      <td>nan</td>\n",
              "      <td>4206.70</td>\n",
              "      <td>NaT</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>6</th>\n",
              "      <td>V9040</td>\n",
              "      <td>&lt;NA&gt;</td>\n",
              "      <td>2025-02-26</td>\n",
              "      <td>1501.79</td>\n",
              "      <td>2025-02-26</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>7</th>\n",
              "      <td>V9057</td>\n",
              "      <td>CL190</td>\n",
              "      <td>2024-13-01</td>\n",
              "      <td>3670.70</td>\n",
              "      <td>NaT</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>8</th>\n",
              "      <td>V9059</td>\n",
              "      <td>CL119</td>\n",
              "      <td>nan</td>\n",
              "      <td>956.36</td>\n",
              "      <td>NaT</td>\n",
              "    </tr>\n",
              "    <tr>\n",
              "      <th>9</th>\n",
              "      <td>V9061</td>\n",
              "      <td>CL124</td>\n",
              "      <td>nan</td>\n",
              "      <td>3923.28</td>\n",
              "      <td>NaT</td>\n",
              "    </tr>\n",
              "  </tbody>\n",
              "</table>\n",
              "</div>\n",
              "    <div class=\"colab-df-buttons\">\n",
              "\n",
              "  <div class=\"colab-df-container\">\n",
              "    <button class=\"colab-df-convert\" onclick=\"convertToInteractive('df-4189312c-c9c7-4167-81be-d7a5fa6e9530')\"\n",
              "            title=\"Convert this dataframe to an interactive table.\"\n",
              "            style=\"display:none;\">\n",
              "\n",
              "  <svg xmlns=\"http://www.w3.org/2000/svg\" height=\"24px\" viewBox=\"0 -960 960 960\">\n",
              "    <path d=\"M120-120v-720h720v720H120Zm60-500h600v-160H180v160Zm220 220h160v-160H400v160Zm0 220h160v-160H400v160ZM180-400h160v-160H180v160Zm440 0h160v-160H620v160ZM180-180h160v-160H180v160Zm440 0h160v-160H620v160Z\"/>\n",
              "  </svg>\n",
              "    </button>\n",
              "\n",
              "  <style>\n",
              "    .colab-df-container {\n",
              "      display:flex;\n",
              "      gap: 12px;\n",
              "    }\n",
              "\n",
              "    .colab-df-convert {\n",
              "      background-color: #E8F0FE;\n",
              "      border: none;\n",
              "      border-radius: 50%;\n",
              "      cursor: pointer;\n",
              "      display: none;\n",
              "      fill: #1967D2;\n",
              "      height: 32px;\n",
              "      padding: 0 0 0 0;\n",
              "      width: 32px;\n",
              "    }\n",
              "\n",
              "    .colab-df-convert:hover {\n",
              "      background-color: #E2EBFA;\n",
              "      box-shadow: 0px 1px 2px rgba(60, 64, 67, 0.3), 0px 1px 3px 1px rgba(60, 64, 67, 0.15);\n",
              "      fill: #174EA6;\n",
              "    }\n",
              "\n",
              "    .colab-df-buttons div {\n",
              "      margin-bottom: 4px;\n",
              "    }\n",
              "\n",
              "    [theme=dark] .colab-df-convert {\n",
              "      background-color: #3B4455;\n",
              "      fill: #D2E3FC;\n",
              "    }\n",
              "\n",
              "    [theme=dark] .colab-df-convert:hover {\n",
              "      background-color: #434B5C;\n",
              "      box-shadow: 0px 1px 3px 1px rgba(0, 0, 0, 0.15);\n",
              "      filter: drop-shadow(0px 1px 2px rgba(0, 0, 0, 0.3));\n",
              "      fill: #FFFFFF;\n",
              "    }\n",
              "  </style>\n",
              "\n",
              "    <script>\n",
              "      const buttonEl =\n",
              "        document.querySelector('#df-4189312c-c9c7-4167-81be-d7a5fa6e9530 button.colab-df-convert');\n",
              "      buttonEl.style.display =\n",
              "        google.colab.kernel.accessAllowed ? 'block' : 'none';\n",
              "\n",
              "      async function convertToInteractive(key) {\n",
              "        const element = document.querySelector('#df-4189312c-c9c7-4167-81be-d7a5fa6e9530');\n",
              "        const dataTable =\n",
              "          await google.colab.kernel.invokeFunction('convertToInteractive',\n",
              "                                                    [key], {});\n",
              "        if (!dataTable) return;\n",
              "\n",
              "        const docLinkHtml = 'Like what you see? Visit the ' +\n",
              "          '<a target=\"_blank\" href=https://colab.research.google.com/notebooks/data_table.ipynb>data table notebook</a>'\n",
              "          + ' to learn more about interactive tables.';\n",
              "        element.innerHTML = '';\n",
              "        dataTable['output_type'] = 'display_data';\n",
              "        await google.colab.output.renderOutput(dataTable, element);\n",
              "        const docLink = document.createElement('div');\n",
              "        docLink.innerHTML = docLinkHtml;\n",
              "        element.appendChild(docLink);\n",
              "      }\n",
              "    </script>\n",
              "  </div>\n",
              "\n",
              "\n",
              "    </div>\n",
              "  </div>\n"
            ],
            "application/vnd.google.colaboratory.intrinsic+json": {
              "type": "dataframe",
              "summary": "{\n  \"name\": \"engine)\",\n  \"rows\": 10,\n  \"fields\": [\n    {\n      \"column\": \"id_venta\",\n      \"properties\": {\n        \"dtype\": \"string\",\n        \"num_unique_values\": 10,\n        \"samples\": [\n          \"V9059\",\n          \"V9012\",\n          \"V9035\"\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    },\n    {\n      \"column\": \"id_cliente\",\n      \"properties\": {\n        \"dtype\": \"string\",\n        \"num_unique_values\": 10,\n        \"samples\": [\n          \"CL119\",\n          \"CL214\",\n          \"CL166\"\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    },\n    {\n      \"column\": \"fecha\",\n      \"properties\": {\n        \"dtype\": \"string\",\n        \"num_unique_values\": 7,\n        \"samples\": [\n          \"2024-01-26\",\n          \"nan\",\n          \"2025-02-26\"\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    },\n    {\n      \"column\": \"total\",\n      \"properties\": {\n        \"dtype\": \"number\",\n        \"std\": 1340.1464079159412,\n        \"min\": 956.36,\n        \"max\": 4206.7,\n        \"num_unique_values\": 8,\n        \"samples\": [\n          4083.42,\n          3670.7,\n          1443.82\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    },\n    {\n      \"column\": \"fecha_dt\",\n      \"properties\": {\n        \"dtype\": \"date\",\n        \"min\": \"2024-01-26 00:00:00\",\n        \"max\": \"2025-02-26 00:00:00\",\n        \"num_unique_values\": 3,\n        \"samples\": [\n          \"2024-01-26 00:00:00\",\n          \"2024-10-14 00:00:00\",\n          \"2025-02-26 00:00:00\"\n        ],\n        \"semantic_type\": \"\",\n        \"description\": \"\"\n      }\n    }\n  ]\n}"
            }
          },
          "metadata": {},
          "execution_count": 47
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from google.colab import files\n",
        "files.download(\"ventas_curated.csv\")\n",
        "files.download(\"ventas_rejects.csv\")"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 17
        },
        "id": "Z2E-z8RI7dfn",
        "outputId": "52171fc1-644e-4c50-a63e-c45123eca7bb"
      },
      "execution_count": 48,
      "outputs": [
        {
          "output_type": "display_data",
          "data": {
            "text/plain": [
              "<IPython.core.display.Javascript object>"
            ],
            "application/javascript": [
              "\n",
              "    async function download(id, filename, size) {\n",
              "      if (!google.colab.kernel.accessAllowed) {\n",
              "        return;\n",
              "      }\n",
              "      const div = document.createElement('div');\n",
              "      const label = document.createElement('label');\n",
              "      label.textContent = `Downloading \"${filename}\": `;\n",
              "      div.appendChild(label);\n",
              "      const progress = document.createElement('progress');\n",
              "      progress.max = size;\n",
              "      div.appendChild(progress);\n",
              "      document.body.appendChild(div);\n",
              "\n",
              "      const buffers = [];\n",
              "      let downloaded = 0;\n",
              "\n",
              "      const channel = await google.colab.kernel.comms.open(id);\n",
              "      // Send a message to notify the kernel that we're ready.\n",
              "      channel.send({})\n",
              "\n",
              "      for await (const message of channel.messages) {\n",
              "        // Send a message to notify the kernel that we're ready.\n",
              "        channel.send({})\n",
              "        if (message.buffers) {\n",
              "          for (const buffer of message.buffers) {\n",
              "            buffers.push(buffer);\n",
              "            downloaded += buffer.byteLength;\n",
              "            progress.value = downloaded;\n",
              "          }\n",
              "        }\n",
              "      }\n",
              "      const blob = new Blob(buffers, {type: 'application/binary'});\n",
              "      const a = document.createElement('a');\n",
              "      a.href = window.URL.createObjectURL(blob);\n",
              "      a.download = filename;\n",
              "      div.appendChild(a);\n",
              "      a.click();\n",
              "      div.remove();\n",
              "    }\n",
              "  "
            ]
          },
          "metadata": {}
        },
        {
          "output_type": "display_data",
          "data": {
            "text/plain": [
              "<IPython.core.display.Javascript object>"
            ],
            "application/javascript": [
              "download(\"download_25b39d13-c843-4a5a-8f41-d8eee9812fc6\", \"ventas_curated.csv\", 6358)"
            ]
          },
          "metadata": {}
        },
        {
          "output_type": "display_data",
          "data": {
            "text/plain": [
              "<IPython.core.display.Javascript object>"
            ],
            "application/javascript": [
              "\n",
              "    async function download(id, filename, size) {\n",
              "      if (!google.colab.kernel.accessAllowed) {\n",
              "        return;\n",
              "      }\n",
              "      const div = document.createElement('div');\n",
              "      const label = document.createElement('label');\n",
              "      label.textContent = `Downloading \"${filename}\": `;\n",
              "      div.appendChild(label);\n",
              "      const progress = document.createElement('progress');\n",
              "      progress.max = size;\n",
              "      div.appendChild(progress);\n",
              "      document.body.appendChild(div);\n",
              "\n",
              "      const buffers = [];\n",
              "      let downloaded = 0;\n",
              "\n",
              "      const channel = await google.colab.kernel.comms.open(id);\n",
              "      // Send a message to notify the kernel that we're ready.\n",
              "      channel.send({})\n",
              "\n",
              "      for await (const message of channel.messages) {\n",
              "        // Send a message to notify the kernel that we're ready.\n",
              "        channel.send({})\n",
              "        if (message.buffers) {\n",
              "          for (const buffer of message.buffers) {\n",
              "            buffers.push(buffer);\n",
              "            downloaded += buffer.byteLength;\n",
              "            progress.value = downloaded;\n",
              "          }\n",
              "        }\n",
              "      }\n",
              "      const blob = new Blob(buffers, {type: 'application/binary'});\n",
              "      const a = document.createElement('a');\n",
              "      a.href = window.URL.createObjectURL(blob);\n",
              "      a.download = filename;\n",
              "      div.appendChild(a);\n",
              "      a.click();\n",
              "      div.remove();\n",
              "    }\n",
              "  "
            ]
          },
          "metadata": {}
        },
        {
          "output_type": "display_data",
          "data": {
            "text/plain": [
              "<IPython.core.display.Javascript object>"
            ],
            "application/javascript": [
              "download(\"download_6ca26894-13d6-44ab-b24b-047d64dcae1f\", \"ventas_rejects.csv\", 1147)"
            ]
          },
          "metadata": {}
        }
      ]
    }
  ]
}
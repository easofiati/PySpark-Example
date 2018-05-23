# Semantix

Qual o objetivo do comando cache em Spark?
Armazena em memória o resultado de uma operação complexa realizada em um RDD, evitando assim ter que realizá-la novamente para utilizações posteriores.

O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
A principal diferença é que o memória

Qual é a função do SparkContext?
É a conexão entre Spark e o Cluster, calculando a melhor estratégia de distribuição do processo Spark no cluster criar rdd, acumuladores e broadcast

Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
É uma coleção de objetos distruibos e imutável. 

GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
O GroupByKey faz uma redistribuição dos dados antes de fazer o Sort. O reduceByKey agrega a key antes de fazer a varredura dos dados.

Explique o que o código Scala abaixo faz.
val textFile = sc . textFile ( "hdfs://..." )
val counts = textFile . flatMap ( line => line . split ( " " ))
. map ( word => ( word , 1 ))
. reduceByKey ( _ + _ )
counts . saveAsTextFile ( "hdfs://..." )

abre o arquivo texto e transforma em um RDD contendo os dados do arquivo. 
Depois pega o conteudo de cada linha dentro do RDD e faz uma quebra de linha por espaço. 
Depois pega cada palavra e cria o mapeamento de cada palavra (palavra, 1).
Depois agrega por chave e realiza a soma dos valores para cada key.
Escreve em disco o resultado obtido no word count transformando o resultado RDD em texto.


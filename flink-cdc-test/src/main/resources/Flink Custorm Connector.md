# Flink 自定义 connector

## 涉及到的类

1. **DynamicTableSinkFactory**
2. **DynamicTableSink** 
3. **RichSinkFunction**
8. **FactoryUtil** 工厂工具类用于SPI发现自定义connector、format和一些option验证方法
4. **RowData** Flink SQL 内部数据结构
5. **RowData.FiledGetter** 获取RowData的数据 
6. **DataType** Flink SQL 数据类型内部由逻辑数据类型和Hint组成
7. **LogicalDataType** Flink SQL 逻辑数据类型
8. **ChangelogMode** 标记数据增删改

## 参考链接

1. [Flink SQL 数据类型](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/types/)
2. [自定义Connector](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/sourcessinks/)


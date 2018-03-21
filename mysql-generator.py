# /usr/bin/python3
# -*- coding:utf8 -*-

import os
import re
import json


class MysqlParser(object):
    TYPE_WORDS = {
        'byte': 'tinyint',
        'short': 'smallint',
        'int': 'mediumint|int|integer',
        'long': 'bigint',
        'string': '.*char|.*text|.*blob',
        'float': 'float',
        'double': 'double|decimal',
        'date': 'date|datetime|timestamp|time|year',
    }

    @staticmethod
    def fetch(url, db, account, identify=''):
        localhost = 'localhost'
        shell = 'mysqldump -i %s -u%s -p%s --opt -d %s' % (url, account, identify, db)
        if localhost in shell:
            shell = shell.replace(' -i %s' % localhost, '')
        print('shell=%s' % shell)
        return os.popen(shell).read()

    def __init__(self):
        """
        初始化
        """
        '''RE_TABLE：将表分为[表名] [字段声明] [表注释]'''
        self.RE_TABLE = r"%s TABLE `(.+?)` \(([\s\S]*)\) ENGINE(?:[\s\S]+COMMENT='([\s\S]+)')*" % 'CREATE'
        '''RE_FIELD_DEF：将字段拆分为[字段名][字段定义][字段类型][长度][默认值字符串]'''
        self.RE_FIELD_DEF = r"`(.+?)` ((.+?)(?:\((.+?)\))? .*(?:.*DEFAULT (.+))?)"
        '''RE_FIELD_KEY：提取字段键值约束(主键，唯一约束等)'''
        self.RE_FIELD_KEY = r"(.+?) KEY[\s\S]*\(`(.+?)`\)"

    def parse(self, sql):
        """
        解析表结构
        """
        blocks, tables = sql.split('Table structure for table'), []
        for block in blocks:
            groups = re.findall(self.RE_TABLE, block, re.I)
            if 0 == len(groups):
                continue
            group = groups[0]
            dirty_rows = group[1].split(',')
            rows = list(map(lambda x: x.replace('\n', '').strip(), dirty_rows))
            fields = self.__fields(rows)
            table = {'name': group[0], 'fields': fields, 'comment': group[2]}
            tables.append(table)
        return tables

    def __fields(self, rows):
        """
        解析表字段结构
        """
        fields = {}
        for row in rows:
            '''
            字段定义匹配
            '''
            groups = re.findall(self.RE_FIELD_DEF, row, re.I)
            if len(groups) > 0:
                field_name, define, d_type, opt_length, value_line = groups[0]
                '''
                处理默认值
                '''
                dirty_values = value_line.split(' ') if len(value_line) > 0 else []
                default_value = dirty_values[0] if len(dirty_values) > 0 else None
                if default_value:
                    default_value = default_value.strip()
                    if re.match(r'NULL|CURRENT_TIMESTAMP', default_value, re.I):
                        default_value = None
                    elif "'" in default_value:
                        default_value = default_value.replace("'", '')
                '''
                处理字段定义
                '''
                define = define.strip().replace('\\n', '') if define else None
                '''
                构造字段字典
                '''
                fields[field_name] = {
                    'define': define,
                    'type': d_type,
                    'length': opt_length,
                    'keys': [],
                    'value': default_value}
                continue
            '''字段关键字约束匹配'''
            groups = re.findall(self.RE_FIELD_KEY, row, re.I)
            if len(groups) > 0:
                key, field_name = groups[0]
                fields[field_name]['keys'].append(key)
                continue
        return fields


class KotlinPlugin(object):

    @staticmethod
    def hump_format(text, split='_', capital=True):
        """
        驼峰格式化
        """
        origin_blocks = text.split(split)
        blocks = []
        for i in range(len(origin_blocks)):
            block = origin_blocks[i]
            if not capital and 0 == i:
                blocks.append(block.lower())
            else:
                blocks.append(block.capitalize())
        return ''.join(blocks)

    @staticmethod
    def kotlin_filed(field_name, field_type, value):
        """
        构建kotlin字段表达式
        """
        field = 'var %s:Any?' % field_name
        for k in MysqlParser.TYPE_WORDS:
            if re.match(MysqlParser.TYPE_WORDS[k], field_type, re.I):
                field_type = k
        '''value值处理'''
        if 'byte' == field_type:
            if value is None:
                field = 'var %s: Byte?' % field_name
            else:
                field = 'var %s: Byte? = %s' % (field_name, value)
        elif 'short' == field_type:
            if value is None:
                field = 'var %s: Short,' % field_name
            else:
                field = 'var %s: Short? = %s' % (field_name, value)
        elif 'int' == field_type:
            if value is None:
                field = 'var %s: Int?' % field_name
            else:
                field = 'var %s: Int? = %s' % (field_name, value)
        elif 'long' == field_type:
            if value is None:
                field = 'var %s: Long,' % field_name
            else:
                field = 'var %s: Long? = %sl' % (field_name, value)
        elif 'string' == field_type:
            if value is None:
                field = 'var %s: String?' % field_name
            else:
                field = 'var %s: String? = "%s"' % (field_name, value)
        elif 'float' == field_type:
            if value is None:
                field = 'var %s: Float?' % field_name
            else:
                field = 'var %s: Float? = %sf' % (field_name, value)
        elif 'double' == field_type:
            if value is None:
                field = 'var %s: Double?' % field_name
            else:
                field = 'var %s: Double? = %s' % (field_name, value)
        elif 'date' == field_type:
            if value is None:
                field = 'var %s: Date?' % field_name
            else:
                field = 'var %s: Date? = %s' % (field_name, value)
        return field

    @staticmethod
    def generate(tables, params):
        """
        生成表结构对应的kotlin-jpa实体类文件
        """
        global class_name, content_lines
        package = params.get('src:package', '')
        path = params.get('src:path', '')
        target_tables = params.get('db.tables', [])
        package_line = 'package %s\n' % package
        '''文件路径'''
        layers = package.split('.')
        for layer in layers:
            path = os.path.join(path, layer)
        '''遍历数据库表'''
        for table in tables:
            content_lines = []
            '''解析表'''
            table_name = table.get('name', '')
            if len(target_tables) > 0 and (table_name not in target_tables):
                '''目标表不为空，且当前表不在目标表中则不处理'''
                continue
            class_name = KotlinPlugin.hump_format(table_name)
            class_explain = table.get('comment', '')
            if len(class_explain) > 0:
                content_lines.append('/**%s*/' % class_explain)
            content_lines.append('@Entity')
            content_lines.append('@Table(name = "`%s`")' % table_name)
            content_lines.append('data class %s(' % class_name)
            '''解析字段'''
            fields = table.get('fields', {})
            for key in fields:
                kv = fields[key]
                define, d_type = kv.get('define', ''), kv.get('type', '')
                keys, value = kv.get('keys', []), kv.get('value', None)
                field_name = KotlinPlugin.hump_format(key, capital=False)
                '''检测主键注解'''
                if 'PRIMARY' in keys:
                    content_lines.append('\t\t@Id')
                '''检测生成策略'''
                if 'AUTO_INCREMENT' in define:
                    strategy = params.get('jpa:generation-type', '')
                    strategy = strategy.upper() if len(strategy) > 0 else 'IDENTITY'
                    content_lines.append('\t\t@GeneratedValue(strategy = GenerationType.%s)' % strategy)
                '''唯一约束检测'''
                unique = ' unique = true,' if 'UNIQUE' in keys else ''
                '''防止与数据库关键字冲突'''
                safe_name = '`%s`' % key
                column = '\t\t@Column(name = "%s",%s columnDefinition = "%s")' % (safe_name, unique, define)
                content_lines.append(column)
                content_lines.append('\t\t%s,' % KotlinPlugin.kotlin_filed(field_name, d_type, value))
            if ',' in content_lines[-1]:
                content_lines[-1] = content_lines[-1].replace(',', '){')
            '''构建无参构造函数'''
            params_null = ['null'] * len(fields)
            content_lines.append('\tconstructor() : this(%s)' % ', '.join(params_null))
            content_lines.append('}')
            '''换行连接content'''
            class_body = '\n'.join(content_lines)
            head_lines = [package_line, 'import javax.persistence.*']
            if 'Date' in class_body:
                head_lines.append('import java.util.*')
            head_lines.append('\n')
            class_head = '\n'.join(head_lines)
            whole_class = '\n'.join([class_head, class_body])
            '''文件写入'''
            file_path = os.path.join(path, '%s.kt' % class_name)
            with open(file_path, 'w') as ktf:
                ktf.write(whole_class)
                print('success write: %s' % file_path)


if __name__ == '__main__':
    file = None
    try:
        file = open('local.config.json')
        config = json.load(file)
        host, name = config['db:host'], config['db:name']
        user, password = config['db:user'], config['db:password']
        content = MysqlParser.fetch(host, name, user, password)
        with open('restaurant.sql', 'w') as f:
            f.write(content)
        parser = MysqlParser()
        result = parser.parse(content)
        KotlinPlugin.generate(result, config)
    except BaseException as exp:
        print(exp)
    finally:
        if file:
            file.close()

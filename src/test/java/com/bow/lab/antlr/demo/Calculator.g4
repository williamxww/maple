
grammar Calculator;

statement
 : expr|NEWLINE
 ;

expr
// 注意此处是先匹配上乘除后加减
 : expr op=(MULT|DIV) expr      # MulDiv
 | expr op=(PLUS|MINUS) expr    # AddSub
 | INT                          # int
 | LPAR expr RPAR               # parens
 ;


NEWLINE         :'\r'?'\n';
LINE_COMMENT    : '//' .*? '\n' -> skip ;
COMMENT         : '/*' .*? '*/' -> skip ;
WS              : [ \t\n\r]+ -> skip ;

INT   : '0'..'9'+;
PLUS  : '+';
MINUS : '-';
MULT  : '*';
DIV   : '/';
LPAR  : '(';
RPAR  : ')';


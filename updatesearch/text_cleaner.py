def clean_text(text):
    """
    Retorna versões das strings que contém caracteres diferentes de letras,
    por exemplo, no texto:
    '“Quem não se comunica se trumbica”: comportamento decisório e estratégias de autopromoção do Supremo Tribunal Federal1', as strings são:
    - “Quem
    - trumbica”:
    - Federal1

    As versões são alterações na string original com intuito de remover caracteres indesejáveis.

    - “Quem se converte a Quem
    - trumbica”: se converte a trumbica” e trumbica
    - Federal1 se converte a Federal

    Observe a entrada e a saída, que contém as strings convertidas

    In : clean_text('“Quem não se comunica se trumbica”: comportamento decisório e estratégias de autopromoção do Supremo Tribunal Federal1')
    Out: 'trumbica Federal trumbica” Quem'

    """
    # mantém todas as str originais
    words = [w.strip() for w in text.split() if w.strip()]

    # verifica se todas as str são alpha, retorna None
    if "".join(words).isalpha():
        return None

    new_words = set()
    to_fix = _get_non_alpha_words(words)
    for word in to_fix:

        if not word:
            continue

        # obtém versões alphanum e alpha da str word e a adiciona se for inédita
        for w in _fix_words(word):
            for part in w.split():
                if part not in words:
                    new_words.add(part)

        # obtém versão da str sem o caracter final se ele não for alpha e a adiciona se for inédita
        if word and not word[-1].isalpha():
            if word[:-1] and word[:-1] not in words:
                new_words.add(word[:-1])
    return " ".join(new_words)


def _get_non_alpha_words(words):
    for word in words:
        if word and not word.isalpha():
            yield word


def _fix_words(word):
    """
    Cria versões alphanum de uma str que não é alphanum,
    ou seja, remove os caracteres para torná-la alphanum,
    e a adiciona em words
    """
    for replace_by in ("", " "):
        if not word.isalnum():
            yield _replace_punctuations(word, replace_by)
            yield "".join(_replace_non_alphanum(word, replace_by))
        if not word.isalpha():
            yield "".join(_replace_non_alpha(word, replace_by))


def _replace_punctuations(text, replace_by):
    """
    Modifica text substituindo pontuações pelo caracter fornecido em replace_by,
    inclusive str vazia
    """
    for item in '’“”?!:;}{[]()…-—"' + "'":
        try:
            text = text.replace(item, replace_by)
        except Exception as e:
            pass
    return text


def _replace_non_alphanum(text, replace_by):
    for c in text:
        if c.isalnum():
            yield c
        else:
            if replace_by:
                yield replace_by


def _replace_non_alpha(text, replace_by):
    for c in text:
        if c.isalpha():
            yield c
        else:
            if replace_by:
                yield replace_by

// @flow

import React from "react";
import AutoLinker from "autolinker";

import classNames from "classnames";
import injectSheet from "react-jss";
import { connect } from "react-redux";
import { navigate } from "redux-url";

type Match = {
    getType(): string,
    getMatchedText(): string,
    getOffset(): number,
    getUrl(): string
};

const styles = {
    color: 'inherit'
};

const buildLinkTag = (m: Match): any => {
    if (m.getType() === "url") return <a style={styles} href={m.getUrl()}>{m.getUrl()}</a>;
    return m.getMatchedText();
}

/**
 * Inlines <a> elements in a string whenever a url is matched
 * 
 * @param {string} text the text to highlight
 */
export const highlightURLs = (text: string): any[] => {
    if (!text) { return []; }
    const matches: Match[] = AutoLinker.parse(text),
        newHtml = [];

    let lastIndex = 0;

    for (let i = 0, len = matches.length; i < len; i++) {
        const match = matches[i];

        newHtml.push(text.substring(lastIndex, match.getOffset()));

        newHtml.push(buildLinkTag(match));

        lastIndex = match.getOffset() + match.getMatchedText().length;
    }
    newHtml.push(text.substring(lastIndex));

    return newHtml;
}
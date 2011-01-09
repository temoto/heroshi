--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: metadata; Type: TABLE; Schema: public; Owner: temoto; Tablespace: 
--

CREATE TABLE metadata (
    url text NOT NULL,
    visited timestamp(0) without time zone,
    status_code smallint,
    fetch_time integer,
    result text,
    headers text,
    content bytea,
    var text,
    content_length integer,
    content_type text,
    urlhash bytea NOT NULL,
    CONSTRAINT metadata_url_check CHECK ((btrim(url) <> ''::text))
);
ALTER TABLE ONLY metadata ALTER COLUMN urlhash SET STORAGE PLAIN;


ALTER TABLE public.metadata OWNER TO temoto;

--
-- Name: metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: temoto; Tablespace: 
--

ALTER TABLE ONLY metadata
    ADD CONSTRAINT metadata_pkey PRIMARY KEY (urlhash);


--
-- Name: metadata_content_b_empty; Type: INDEX; Schema: public; Owner: temoto; Tablespace: 
--

CREATE INDEX metadata_content_empty ON metadata USING btree ((((content IS NULL) OR (content = ''::bytea))));


--
-- Name: metadata_result; Type: INDEX; Schema: public; Owner: temoto; Tablespace: 
--

CREATE INDEX metadata_result ON metadata USING btree (result);


--
-- Name: metadata_status_code; Type: INDEX; Schema: public; Owner: temoto; Tablespace: 
--

CREATE INDEX metadata_status_code ON metadata USING btree (status_code);


--


--
-- Name: metadata_url; Type: INDEX; Schema: public; Owner: temoto; Tablespace: 
--

CREATE INDEX metadata_url ON metadata USING btree (url);


--
-- Name: metadata_urlhash; Type: INDEX; Schema: public; Owner: temoto; Tablespace: 
--

CREATE UNIQUE INDEX metadata_urlhash ON metadata USING btree (urlhash);


--
-- Name: metadata_visited_d; Type: INDEX; Schema: public; Owner: temoto; Tablespace: 
--

CREATE INDEX metadata_visited_d ON metadata USING btree (visited);


--
-- Name: metadata_visited_nf; Type: INDEX; Schema: public; Owner: temoto; Tablespace: 
--

CREATE INDEX metadata_visited_nf ON metadata USING btree (visited NULLS FIRST);


--
-- PostgreSQL database dump complete
--


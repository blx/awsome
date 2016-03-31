package com.kiblerdude.awsome.cloudsearch;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.entity.ContentType;

import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsRequest;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

/**
 * Builds AWS Cloudsearch {@link UploadDocumentsRequest}.
 * <p>
 * The builder provides easy ways to add and remove documents from the
 * Cloudsearch index.
 * 
 * @author kiblerj
 * 
 * @param <T>
 *            A class representing the documents in the Cloudsearch schema,
 *            properly annotated with Jackson annotations.
 */
public final class UploadDocumentsBuilder<T extends Object> {
	
	private ImmutableSet<UploadAction<T>> adds;
	private ImmutableSet<UploadAction<T>> deletes;
	
	/**
	 * Constructor
	 */
	public UploadDocumentsBuilder() {
		adds = ImmutableSet.of();
		deletes = ImmutableSet.of();
	}
	
	/**
	 * Builds a {@link UploadDocumentsRequest} with the documents added or deleted.
	 * @return {@link UploadDocumentsRequest}
	 */
	public UploadDocumentsRequest build() {
		UploadDocumentsRequest request = new UploadDocumentsRequest();
		request.setContentType(ContentType.APPLICATION_JSON.getMimeType());
		try {
			ObjectMapper mapper = new ObjectMapper();
			List<UploadAction<T>> actions = new ArrayList<>(adds.size() + deletes.size());
			actions.addAll(adds.asList());
			actions.addAll(deletes.asList());
			UploadActionBatch<T> batch = new UploadActionBatch<>(actions);
			String json = mapper.writeValueAsString(batch);
			request.setContentLength((long)json.length());
			request.setDocuments(new StringInputStream(json));
			return request;
		} catch (JsonProcessingException | UnsupportedEncodingException e) {
			return null;
		}
	}
	
	/**
	 * Adds the <code>document</code> with the specified <code>id</code> to the
	 * Cloudsearch index.
	 * 
	 * @param id
	 *            The id of the document being added to the Cloudsearch index.
	 * @param document
	 *            The document being added to the Cloudsearch index.
	 * @return UploadDocumentsBuilder
	 */
	public UploadDocumentsBuilder<T> add(String id, T document) {
		checkNotNull(id, "Document id is null");
		checkNotNull(document, "Document to add is null");
		ImmutableSet.Builder<UploadAction<T>> builder = new ImmutableSet.Builder<>();
		builder.addAll(adds);
		builder.add(new UploadAction<>(id, document));
		adds = builder.build();
		return this;
	}

	/**
	 * Adds one or more <code>documents</code> to the Cloudsearch index.
	 * 
	 * @param documents
	 *            A mapping of document id's to documents being added to the
	 *            Cloudsearch index.
	 * @return UploadDocumentsBuilder
	 */
	public UploadDocumentsBuilder<T> add(Map<String, T> documents) {
		checkNotNull(documents, "Documents to add is null");
		ImmutableSet.Builder<UploadAction<T>> builder = new ImmutableSet.Builder<>();
		builder.addAll(adds);
		for (Entry<String, T> entry : documents.entrySet()) {
			builder.add(new UploadAction<T>(entry.getKey(), entry.getValue()));
		}
		adds = builder.build();
		return this;
	}
	
	/**
	 * Deletes one ore more ids from the Cloudsearch index.
	 * 
	 * @param ids
	 *            The document ids to delete from the Cloudsearch index.
	 * @return UploadDocumentsBuilder
	 */
	public UploadDocumentsBuilder<T> delete(Iterable<String> ids) {
		checkNotNull(ids, "Document ids to delete are null");
		ImmutableSet.Builder<UploadAction<T>> builder = new ImmutableSet.Builder<>();
		builder.addAll(deletes);
		for (String id : ids) {
			builder.add(new UploadAction<T>(id));
		}
		deletes = builder.build();
		return this;
	}
	
	/**
	 * Deletes one ore more ids from the Cloudsearch index.
	 * 
	 * @param ids
	 *            The document ids to delete from the Cloudsearch index.
	 * @return UploadDocumentsBuilder
	 */
	public UploadDocumentsBuilder<T> delete(String... ids) {
		checkNotNull(ids, "Document ids to delete are null");
		return delete(Arrays.asList(ids));
	}
}

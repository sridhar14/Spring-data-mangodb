/*
 * Copyright 2021. the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2017-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.Data;

import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.ManualReference;
import org.springframework.data.mongodb.core.mapping.ObjectReference;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.lang.Nullable;

import com.mongodb.client.MongoClient;

/**
 * {@link DBRef} related integration tests for {@link MongoTemplate}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MongoClientExtension.class)
public class MongoTemplateManualReferenceTests {

	public static final String DB_NAME = "manual-reference-tests";

	static @Client MongoClient client;

	MongoTestTemplate template = new MongoTestTemplate(cfg -> {

		cfg.configureDatabaseFactory(it -> {

			it.client(client);
			it.defaultDb(DB_NAME);
		});

		cfg.configureConversion(it -> {
			it.customConverters(new ManualReferenceConverter());
		});

		cfg.configureMappingContext(it -> {
			it.autocreateIndex(false);
		});
	});

	@BeforeEach
	public void setUp() {
		template.flush(Root.class, JustSomeType.class);
	}

	@Test
	void xxx() {

		JustSomeType justSomeType = new JustSomeType();
		justSomeType.id = "id-1";
		justSomeType.value = "v-1";

		template.save(justSomeType);

		JustSomeType2 justSomeType2 = new JustSomeType2();
		justSomeType2.id = "t2";
		justSomeType2.value = "v-2";

		template.save(justSomeType2);

		Root root = new Root();
		root.id = "r-1";
		root.value = "rv-1";
		root.refValue = justSomeType;
		root.refValueIndexPlaceholder = justSomeType;
		root.refValueIndexPlaceholderSpel = justSomeType2;
		root.refValue2 = justSomeType2;
//		root.refValueList = Collections.singletonList(justSomeType);

		template.save(root);

		List<Root> result = template.find(query(where("id").is(root.id)), Root.class);
		System.out.println("result: " + result);
	}

	@Data
	static class Root {

		@Id String id;
		String value;


		/*

		{
			_id : ..
			value : ..
			refValue : id-2

		}

		 */
		// refValue : id-2 | { id : id-2, col : coll-1 }
		// _id : ?#{refValue.id}

		@ManualReference(lookup = "{ '_id' : '?#{#this}' }") // vs '?#{refValue}' vs target as variable name
		JustSomeType refValue;

		// refValue : { id : id-2, col : coll-1 }
		@ManualReference(lookup = "{ '_id' : '?#{id}' }") // vs ?#{#this[id]}
		JustSomeType2 refValue2;

		@ManualReference(lookup = "{ '_id' : '?0' }") // strange case
		JustSomeType refValueIndexPlaceholder;

		// TODO: still need to bind the named parameters - sigh - why oh why?
		@ManualReference(lookup = "{ '_id' : '?#{#this.id}' }") // should this work #{#id} check also target.id
		JustSomeType2 refValueIndexPlaceholderSpel;

		// ok and we need some sort of list handlign
		// not working by now

		// ripple load for single elements { '_id' : '?#{#this}' }
		// intern concat with OR on Mark you are a genius!!!
		@ManualReference(lookup = "{ '_id' : { $in : ?#{#this} } }", collection = "#{collection}")
		List<JustSomeType> refValueList;
	}

	interface Identifyable {
		String getId();
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document("justSomeType")
	static class JustSomeType implements Identifyable {

		@Id String id;
		String value;
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document("justSomeType")
	static class JustSomeType2 implements Identifyable {

		@Id String id;
		String value;
	}

	static class ManualReferenceConverter implements Converter<Identifyable, ObjectReference> {

		@Nullable
		@Override
		public ObjectReference convert(Identifyable source) {

			if (source instanceof JustSomeType2) {
				return () -> new Document("collection", "justSomeType").append("id", source.getId());
			}
			return source::getId;
		}
	}

}

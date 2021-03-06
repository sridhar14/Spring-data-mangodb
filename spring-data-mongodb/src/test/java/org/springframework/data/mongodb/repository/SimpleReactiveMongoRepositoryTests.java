/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.domain.ExampleMatcher.*;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.With;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;

import javax.annotation.Nullable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.repository.support.ReactiveMongoRepositoryFactory;
import org.springframework.data.mongodb.repository.support.SimpleReactiveMongoRepository;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.ClassUtils;

/**
 * Tests for {@link ReactiveMongoRepository}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Ruben J Garcia
 * @author Cl??ment Petit
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class SimpleReactiveMongoRepositoryTests implements BeanClassLoaderAware, BeanFactoryAware {

	@Autowired private ReactiveMongoTemplate template;

	private ReactiveMongoRepositoryFactory factory;
	private ClassLoader classLoader;
	private BeanFactory beanFactory;
	private ReactivePersonRepository repository;
	private ReactiveImmutablePersonRepository immutableRepository;

	private ReactivePerson dave, oliver, carter, boyd, stefan, leroi, alicia;
	private ImmutableReactivePerson keith, james, mariah;

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader == null ? ClassUtils.getDefaultClassLoader() : classLoader;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	@BeforeEach
	void setUp() {

		factory = new ReactiveMongoRepositoryFactory(template);
		factory.setRepositoryBaseClass(SimpleReactiveMongoRepository.class);
		factory.setBeanClassLoader(classLoader);
		factory.setBeanFactory(beanFactory);
		factory.setEvaluationContextProvider(ReactiveQueryMethodEvaluationContextProvider.DEFAULT);

		repository = factory.getRepository(ReactivePersonRepository.class);
		immutableRepository = factory.getRepository(ReactiveImmutablePersonRepository.class);

		repository.deleteAll().as(StepVerifier::create).verifyComplete();
		immutableRepository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave = new ReactivePerson("Dave", "Matthews", 42);
		oliver = new ReactivePerson("Oliver August", "Matthews", 4);
		carter = new ReactivePerson("Carter", "Beauford", 49);
		boyd = new ReactivePerson("Boyd", "Tinsley", 45);
		stefan = new ReactivePerson("Stefan", "Lessard", 34);
		leroi = new ReactivePerson("Leroi", "Moore", 41);
		alicia = new ReactivePerson("Alicia", "Keys", 30);
		keith = new ImmutableReactivePerson(null, "Keith", "Urban", 53);
		james = new ImmutableReactivePerson(null, "James", "Arthur", 33);
		mariah = new ImmutableReactivePerson(null, "Mariah", "Carey", 51);

		repository.saveAll(Arrays.asList(oliver, dave, carter, boyd, stefan, leroi, alicia)).as(StepVerifier::create) //
				.expectNextCount(7) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	void existsByIdShouldReturnTrueForExistingObject() {
		repository.existsById(dave.id).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void existsByIdShouldReturnFalseForAbsentObject() {
		repository.existsById("unknown").as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void existsByMonoOfIdShouldReturnTrueForExistingObject() {
		repository.existsById(Mono.just(dave.id)).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1712
	void existsByFluxOfIdShouldReturnTrueForExistingObject() {
		repository.existsById(Flux.just(dave.id, oliver.id)).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void existsByEmptyMonoOfIdShouldReturnEmptyMono() {
		repository.existsById(Mono.empty()).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findByIdShouldReturnObject() {
		repository.findById(dave.id).as(StepVerifier::create).expectNext(dave).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findByIdShouldCompleteWithoutValueForAbsentObject() {
		repository.findById("unknown").as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findByIdByMonoOfIdShouldReturnTrueForExistingObject() {
		repository.findById(Mono.just(dave.id)).as(StepVerifier::create).expectNext(dave).verifyComplete();
	}

	@Test // DATAMONGO-1712
	void findByIdByFluxOfIdShouldReturnTrueForExistingObject() {
		repository.findById(Flux.just(dave.id, oliver.id)).as(StepVerifier::create).expectNext(dave).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findByIdByEmptyMonoOfIdShouldReturnEmptyMono() {
		repository.findById(Mono.empty()).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findAllShouldReturnAllResults() {
		repository.findAll().as(StepVerifier::create).expectNextCount(7).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findAllByIterableOfIdShouldReturnResults() {
		repository.findAllById(Arrays.asList(dave.id, boyd.id)).as(StepVerifier::create).expectNextCount(2)
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findAllByPublisherOfIdShouldReturnResults() {
		repository.findAllById(Flux.just(dave.id, boyd.id)).as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findAllByEmptyPublisherOfIdShouldReturnResults() {
		repository.findAllById(Flux.empty()).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findAllWithSortShouldReturnResults() {

		repository.findAll(Sort.by(new Order(Direction.ASC, "age"))).as(StepVerifier::create) //
				.expectNextCount(7) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	void countShouldReturnNumberOfRecords() {
		repository.count().as(StepVerifier::create).expectNext(7L).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void insertEntityShouldInsertEntity() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		repository.insert(person).as(StepVerifier::create).expectNext(person).verifyComplete();

		assertThat(person.getId()).isNotNull();
	}

	@Test // DATAMONGO-1444
	void insertShouldDeferredWrite() {

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		repository.insert(person);

		assertThat(person.getId()).isNull();
	}

	@Test // DATAMONGO-1444
	void insertIterableOfEntitiesShouldInsertEntity() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		repository.insert(Arrays.asList(dave, oliver, boyd)).as(StepVerifier::create) //
				.expectNext(dave, oliver, boyd) //
				.verifyComplete();

		assertThat(dave.getId()).isNotNull();
		assertThat(oliver.getId()).isNotNull();
		assertThat(boyd.getId()).isNotNull();
	}

	@Test // DATAMONGO-1444
	void insertPublisherOfEntitiesShouldInsertEntity() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		repository.insert(Flux.just(dave, oliver, boyd)).as(StepVerifier::create).expectNextCount(3).verifyComplete();

		assertThat(dave.getId()).isNotNull();
		assertThat(oliver.getId()).isNotNull();
		assertThat(boyd.getId()).isNotNull();
	}

	@Test // DATAMONGO-1444
	void saveEntityShouldUpdateExistingEntity() {

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		repository.save(dave).as(StepVerifier::create).expectNext(dave).verifyComplete();

		repository.findByLastname("Matthews").as(StepVerifier::create).expectNext(oliver).verifyComplete();

		repository.findById(dave.id).as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual.getFirstname()).isEqualTo(dave.getFirstname());
			assertThat(actual.getLastname()).isEqualTo(dave.getLastname());
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void saveEntityShouldInsertNewEntity() {

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		repository.save(person).as(StepVerifier::create).expectNext(person).verifyComplete();

		repository.findById(person.id).as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual.getFirstname()).isEqualTo(person.getFirstname());
			assertThat(actual.getLastname()).isEqualTo(person.getLastname());
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void saveIterableOfNewEntitiesShouldInsertEntity() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		repository.saveAll(Arrays.asList(dave, oliver, boyd)).as(StepVerifier::create).expectNextCount(3).verifyComplete();

		assertThat(dave.getId()).isNotNull();
		assertThat(oliver.getId()).isNotNull();
		assertThat(boyd.getId()).isNotNull();
	}

	@Test // DATAMONGO-1444
	void saveIterableOfMixedEntitiesShouldInsertEntity() {

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		repository.saveAll(Arrays.asList(person, dave)).as(StepVerifier::create).expectNextCount(2).verifyComplete();

		repository.findById(dave.id).as(StepVerifier::create).expectNext(dave).verifyComplete();

		assertThat(person.id).isNotNull();
		repository.findById(person.id).as(StepVerifier::create).expectNext(person).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void savePublisherOfEntitiesShouldInsertEntity() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		repository.saveAll(Flux.just(dave, oliver, boyd)).as(StepVerifier::create).expectNextCount(3).verifyComplete();

		assertThat(dave.getId()).isNotNull();
		assertThat(oliver.getId()).isNotNull();
		assertThat(boyd.getId()).isNotNull();
	}

	@Test // GH-3609
	void savePublisherOfImmutableEntitiesShouldInsertEntity() {

		immutableRepository.deleteAll().as(StepVerifier::create).verifyComplete();

		immutableRepository.saveAll(Flux.just(keith)).as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.id).isNotNull();
				}) //
			.verifyComplete();
	}

	@Test // DATAMONGO-1444
	void deleteAllShouldRemoveEntities() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		repository.findAll().as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void deleteByIdShouldRemoveEntity() {

		repository.deleteById(dave.id).as(StepVerifier::create).verifyComplete();

		repository.findById(dave.id).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1712
	void deleteByIdUsingMonoShouldRemoveEntity() {

		repository.deleteById(Mono.just(dave.id)).as(StepVerifier::create).verifyComplete();

		repository.existsById(dave.id).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-1712
	void deleteByIdUsingFluxShouldRemoveEntity() {

		repository.deleteById(Flux.just(dave.id, oliver.id)).as(StepVerifier::create).verifyComplete();

		repository.existsById(dave.id).as(StepVerifier::create).expectNext(false).verifyComplete();
		repository.existsById(oliver.id).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void deleteShouldRemoveEntity() {

		repository.delete(dave).as(StepVerifier::create).verifyComplete();

		repository.findById(dave.id).as(StepVerifier::create).verifyComplete();

	}

	@Test // DATAMONGO-1444
	void deleteIterableOfEntitiesShouldRemoveEntities() {

		repository.deleteAll(Arrays.asList(dave, boyd)).as(StepVerifier::create).verifyComplete();

		repository.findById(boyd.id).as(StepVerifier::create).verifyComplete();

		repository.findByLastname("Matthews").as(StepVerifier::create).expectNext(oliver).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void deletePublisherOfEntitiesShouldRemoveEntities() {

		repository.deleteAll(Flux.just(dave, boyd)).as(StepVerifier::create).verifyComplete();

		repository.findById(boyd.id).as(StepVerifier::create).verifyComplete();

		repository.findByLastname("Matthews").as(StepVerifier::create).expectNext(oliver).verifyComplete();
	}

	@Test // DATAMONGO-1619
	void findOneByExampleShouldReturnObject() {

		Example<ReactivePerson> example = Example.of(dave);

		repository.findOne(example).as(StepVerifier::create).expectNext(dave).verifyComplete();
	}

	@Test // DATAMONGO-1619
	void findAllByExampleShouldReturnObjects() {

		Example<ReactivePerson> example = Example.of(dave, matching().withIgnorePaths("id", "age", "firstname"));

		repository.findAll(example).as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@Test // DATAMONGO-1619
	void findAllByExampleAndSortShouldReturnObjects() {

		Example<ReactivePerson> example = Example.of(dave, matching().withIgnorePaths("id", "age", "firstname"));

		repository.findAll(example, Sort.by("firstname")).as(StepVerifier::create).expectNext(dave, oliver)
				.verifyComplete();
	}

	@Test // DATAMONGO-1619
	void countByExampleShouldCountObjects() {

		Example<ReactivePerson> example = Example.of(dave, matching().withIgnorePaths("id", "age", "firstname"));

		repository.count(example).as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@Test // DATAMONGO-1619
	void existsByExampleShouldReturnExisting() {

		Example<ReactivePerson> example = Example.of(dave, matching().withIgnorePaths("id", "age", "firstname"));

		repository.exists(example).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1619
	void existsByExampleShouldReturnNonExisting() {

		Example<ReactivePerson> example = Example.of(new ReactivePerson("foo", "bar", -1));

		repository.exists(example).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-1619
	void findOneShouldEmitIncorrectResultSizeDataAccessExceptionWhenMoreThanOneElementFound() {

		Example<ReactivePerson> example = Example.of(new ReactivePerson(null, "Matthews", -1),
				matching().withIgnorePaths("age"));

		repository.findOne(example).as(StepVerifier::create).expectError(IncorrectResultSizeDataAccessException.class);
	}

	@Test // DATAMONGO-1907
	void findOneByExampleWithoutResultShouldCompleteEmpty() {

		Example<ReactivePerson> example = Example.of(new ReactivePerson("foo", "bar", -1));

		repository.findOne(example).as(StepVerifier::create).verifyComplete();
	}

	interface ReactivePersonRepository extends ReactiveMongoRepository<ReactivePerson, String> {

		Flux<ReactivePerson> findByLastname(String lastname);

	}

	interface ReactiveImmutablePersonRepository extends ReactiveMongoRepository<ImmutableReactivePerson, String> {

	}

	@Data
	@NoArgsConstructor
	static class ReactivePerson {

		@Id String id;

		String firstname;
		String lastname;
		int age;

		ReactivePerson(String firstname, String lastname, int age) {

			this.firstname = firstname;
			this.lastname = lastname;
			this.age = age;
		}
	}

	@With
	@Value
	static class ImmutableReactivePerson {

		@Id String id;

		String firstname;
		String lastname;
		int age;

		ImmutableReactivePerson(@Nullable String id, String firstname, String lastname, int age) {
			this.id = id;
			this.firstname = firstname;
			this.lastname = lastname;
			this.age = age;
		}
	}

}

package org.jetbrains.service

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import feign.FeignException
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.jetbrains.recommender.RandomCoffeeApiClient
import org.jetbrains.recommender.model.SuggestCatForRandomCoffeeRequest
import org.jetbrains.repository.Cat
import org.jetbrains.repository.CatBreedRepository
import org.jetbrains.repository.CatRepository
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

data class CatWithBreed(val id: Long, val name: String, val breed: String)
data class NewCat(val name: String, val breed: String)@Service
class CatService(
    private val catRepository: CatRepository,
    private val catBreedRepository: CatBreedRepository,
    private val catRecommenderClient: RandomCoffeeApiClient
) {

    private val log: Logger = LoggerFactory.getLogger(CatService::class.java)
    private val breedCache: MutableMap<Long, CatBreed> = mutableMapOf()
fun generatePairs(limit: Int): List<Pair<CatWithBreed, CatWithBreed>> {
        val breedCache = mutableMapOf<Long, String>()
        
        val catList = catRepository.findAllWithLimit(limit)
        val breedIds = catList.map { it.breedId }.distinct()
        val breedsMap = catBreedRepository.findByIdIn(breedIds).associateBy { it.id }
        breedCache.putAll(breedsMap.mapValues { it.value.name })
        
        val catsWithBreed = catList.map {
            CatWithBreed(
                it.id,
                it.name,
                breedCache[it.breedId] ?: throw RuntimeException("Breed not found")
            )
        }
        
        val catIds = catsWithBreed.map { it.id }
        val recommendationsMap = catIds.associateWith { suggestCat(catsWithBreed.first { cat -> cat.id == it }) }
        val friendIds = recommendationsMap.values.toList()
        val friendCatsMap = catRepository.findByIdIn(friendIds).associateBy { it.id }
        
        val friendBreedIds = friendCatsMap.values.map { it.breedId }.distinct()
        val additionalBreedIds = friendBreedIds.filter { it !in breedCache }
        if (additionalBreedIds.isNotEmpty()) {
            val additionalBreedsMap = catBreedRepository.findByIdIn(additionalBreedIds).associateBy { it.id }
            breedCache.putAll(additionalBreedsMap.mapValues { it.value.name })
        }
        
        return catsWithBreed.map { cat ->
            val friendId = recommendationsMap[cat.id] ?: throw RuntimeException("Friend not found")
            val friend = friendCatsMap[friendId] ?: throw RuntimeException("Friend cat not found")
            cat to CatWithBreed(
                friend.id,
                friend.name,
                breedCache[friend.breedId] ?: throw RuntimeException("Breed not found")
            )
        }
    }@Transactional
    fun addCat(cat: NewCat): CatWithBreed {
        val breed = catBreedRepository.findByName(cat.breed).orElseThrow { RuntimeException("Breed not found") }
        val createdCat = catRepository.save(Cat(0L, breed.id, cat.name, ""))
        return CatWithBreed(createdCat.id, createdCat.name, breed.name)
    }@WithSpan
    private fun suggestCat(cat: CatWithBreed): Long =
        try {
            catRecommenderClient.suggestCat(SuggestCatForRandomCoffeeRequest(cat.id, cat.name, cat.breed)).id
        } catch (e: FeignException) {
            log.error("Failed to suggest cat", e)
            throw CatRecommenderIntegrationException(
                JACKSON_MAPPER.readValue(e.contentUTF8(), FastAPIExceptionResponse::class.java).detail,
                e
            )
        }@WithSpan
    @Transactional
    fun findCatsByName(name: String): List<CatWithBreed> {
        val cats = catRepository.findAllByName(name)
        val breedIds = cats.map { it.breedId }.distinct()
        val breedsMap = catBreedRepository.findByIdIn(breedIds).associateBy { it.id }
        val breedCache = mutableMapOf<Long, String>()
        breedsMap.forEach { (id, breed) -> breedCache[id] = breed.name }
        
        val result = cats.map {
            CatWithBreed(
                it.id,
                it.name,
                breedCache[it.breedId] ?: throw RuntimeException("Breed not found")
            )
        }
        return result
    }

    @WithSpan
    @Transactional
    fun getAllCats(): List<CatWithBreed> {
        val cats = catRepository.findAll()
        val breedIds = cats.map { it.breedId }.distinct()
        val breedsMap = catBreedRepository.findByIdIn(breedIds).associateBy { it.id }
        val breedCache = mutableMapOf<Long, String>()
        breedsMap.forEach { (id, breed) -> breedCache[id] = breed.name }
        
        return cats.map {
            CatWithBreed(
                it.id,
                it.name,
                breedCache[it.breedId] ?: throw RuntimeException("Breed not found")
            )
        }
    }

    @WithSpan
    @Transactional
    fun countCats(): Long = catRepository.countAll()

    companion object {
        private val JACKSON_MAPPER = ObjectMapper()
    }
}data class FastAPIExceptionResponse(@JsonProperty("detail") val detail: String)class CatRecommenderIntegrationException(message: String, exception: Exception) : RuntimeException(message, exception)